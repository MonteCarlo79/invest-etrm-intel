# ERRORS.md — bess-platform

Check this before suggesting approaches to tasks similar to those logged below. If a match is found, skip to what worked.

---

## docker build from OneDrive repo root: context stall + silently missing files (Mac)

**What didn't work:**
1. `docker build .` with the repo on OneDrive as context — hung at `[internal] load build context` (0% CPU, indefinite). OneDrive File Provider stat/hydration stalls the context walk.
2. Staging the context with `rsync -a` — rsync uses `mmap` for reads, and mmap does NOT trigger hydration of dataless OneDrive files. Result: `mmap: Operation timed out` per file, rsync continues past them, and whole directories (e.g. `services/settlement_ingest/`) land in the context as EMPTY dirs. The build succeeds; the app deploys; the tab then dies with `ModuleNotFoundError` in prod. Inspecting `ls | head` of the staged dir is NOT a completeness check — the directory existing ≠ files inside it.

**What worked:**
- Stage the context on local disk with **ditto** (plain `read()` → File Provider hydrates properly), then build from the staged path:
  ```bash
  rm -rf /tmp/ctx && mkdir -p /tmp/ctx/apps
  ditto apps/<app> /tmp/ctx/apps/<app>
  for d in libs services shared auth; do ditto "$d" "/tmp/ctx/$d"; done
  find /tmp/ctx -name "__pycache__" -type d -exec rm -rf {} + ; find /tmp/ctx -name "*Chen*" -delete
  docker build --platform linux/amd64 -f /tmp/ctx/apps/<app>/Dockerfile -t <repo>:<vN> /tmp/ctx
  ```
- **Prove completeness before building:** `diff <(find apps/<app> libs services shared auth -type f | sort) <(cd /tmp/ctx && find ... | sort)` must be empty.
- **Prove the image before pushing:** `docker run --rm --platform linux/amd64 <repo>:<vN> python -c "import <critical modules>"`.
- Note: macOS stock rsync has no `--no-mmap` flag (that would also work where available). First observed 2026-08-10, asset-risk v29 → rebuilt as v30.

**Related (2026-08-11, asset-risk v33):** Docker Hub unreachable from this network (`TLS handshake timeout` on the `FROM python:3.11-slim` metadata Head — buildkit re-checks the registry even when the base image is cached locally). Fix: point the FROM line at AWS's official mirror **in the throwaway build context only** — `FROM public.ecr.aws/docker/library/python:3.11-slim` — and keep the committed Dockerfile on `python:3.11-slim`. Do NOT use third-party mirrors (daocloud etc.) without explicit approval.

---

## docker push to ECR fails: broken pipe through Docker Desktop proxy (Mac)

**What didn't work:**
1. `docker push` retry loop — large blobs (~100–300 MB) died mid-upload with `write tcp ...->192.168.65.1:3128: write: broken pipe`; the same blob failed 3×. docker resumes at layer granularity (whole blob restarts), so big blobs never complete.
2. Restarting Docker Desktop — no change. The 3128 proxy is Docker Desktop's built-in (`http.docker.internal:3128` in `docker info`); macOS had no system proxy and no proxy app running.

**What worked:**
- `docker save <img> -o /tmp/img.tar`, then push from the HOST network with **crane** (bypasses the Docker VM proxy entirely, and crane resumes *within* a blob via PATCH on broken pipes):
  ```bash
  curl -sL -o /tmp/g.tar.gz https://github.com/google/go-containerregistry/releases/latest/download/go-containerregistry_Darwin_arm64.tar.gz
  tar -xzf /tmp/g.tar.gz -C /tmp crane
  aws ecr get-login-password --region ap-southeast-1 | /tmp/crane auth login --username AWS --password-stdin 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com
  /tmp/crane push /tmp/img.tar 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/<repo>:latest
  ```
- Then run only the ECS section of the deploy script manually (register-task-definition + update-service) — re-running the full script would hit the broken `docker push` again.
- Verify: running task's `imageDigest` must equal crane's reported manifest digest.

**Note for next time:** On the Mac, if docker push to ECR dies with broken pipe to `192.168.65.x:3128`, skip retries — go straight to crane. First observed 2026-08-06 during hermes :163 deploy. **crane v0.21.9 is permanently installed at `~/.local/bin/crane`** (no Homebrew on this Mac) — skip the download steps, just `docker save` + `crane auth login` + `crane push`.

**Refinement (2026-08-10, spot-markets v92):** crane's in-blob PATCH resume does NOT always save a push — a 477 MB image over this link (flows die every ~25 min) exhausted crane's own retries and the process died mid-push. Two fixes that worked: (1) rerun `crane push` in a retry loop — already-committed blobs report `existing blob` and are skipped, only incomplete ones re-upload; (2) tag `:latest` server-side with `crane tag repo:v92 latest` (manifest-only, no second upload). Also: interactive zsh does NOT treat mid-line `#` as a comment (`interactivecomments` off) — keep comments out of pasted command lines.

---

## Bulk DB loads from Mac/home network die mid-transfer (RDS blackholed connections)

**What didn't work:**
1. `scripts/ingest_nodal_csvs.py` original design: one connection per file → TEMP staging table → chunked COPY → single INSERT ON CONFLICT. A 3.5M-row file died at 31 min with `psycopg2.OperationalError: could not receive data from server: Operation timed out / SSL SYSCALL error` — the whole file lost (TEMP staging vanishes with the connection).
2. Per-chunk fresh connections ALONE (first patch) — chunks then *hung indefinitely*: the network blackholes sockets silently and macOS takes 15+ min on TCP retransmits before erroring. Retry logic never fired because no error was raised yet.

**What worked:**
- Per-chunk idempotent upserts (`execute_values` + `ON CONFLICT`, 100k rows, fresh connection per chunk, retry once) — a dead connection costs one chunk.
- **Plus** libpq client-side timeouts in `create_engine(connect_args={...})`: `tcp_user_timeout=120000` (error after 2 min of unacked data), `keepalives_idle=30 / interval=10 / count=3`, `connect_timeout=15`. Without `tcp_user_timeout` the hangs persist.
- Vendor CSV quirks handled at load: mixed date-only/`+08:00` metric_time formats and doubled rows (陕西 2026) → reconstruct metric_time as CST midnight + 15min×(slot−1) (same convention as `services/fengxing/nodal_price.py`), dedupe keep-last.

**Note for next time:** On this network, any single DB transaction >10 min will eventually die. Design bulk loads as per-chunk idempotent from the start, and always set `tcp_user_timeout` — server-side `statement_timeout` does nothing for a client stuck SENDING data.

---

## matplotlib font cache not picking up newly installed CJK font

**What didn't work:**
1. Installing `fonts-noto-cjk` before `pip install matplotlib` in Dockerfile — matplotlib's font cache was built during pip install before the font existed, so `findfont("Noto Sans CJK SC")` returned nothing.
2. Calling `matplotlib.font_manager._rebuild()` — method removed in matplotlib 3.7+; raises `AttributeError`.

**What worked:**
- Install pip packages first, then install `fonts-noto-cjk` via apt-get in a subsequent layer.
- Rebuild cache with `python -c "import matplotlib.font_manager as _fm; _fm.fontManager = _fm.FontManager()"` in the same RUN layer as the apt-get install.
- Add file-scan fallback in app code using `findSystemFonts()` + `addfont()` for robustness.

**Note for next time:** Always install system fonts AFTER pip packages in the Dockerfile. Use `FontManager()` reinstantiation, not `_rebuild()`.

---

## pandas Styler.applymap AttributeError

**What didn't work:**
1. `df.style.applymap(fn, subset=cols)` — `applymap` renamed to `map` in pandas 2.1+.
2. `df.style.map(fn, subset=cols)` — still version-sensitive; the built Docker image repeatedly served old cached code, making it hard to verify the fix was deployed.

**What worked:**
- Remove pandas Styler entirely. Format surplus/deficit columns as plain strings with `+` prefix for positive values. No styling dependency, no version sensitivity.

**Note for next time:** Avoid `df.style` in Streamlit apps — version sensitivity between pandas releases causes hard-to-diagnose errors. Use string formatting or Streamlit's `column_config` instead.

---

## Docker COPY layer caching old code despite --no-cache

**What didn't work:**
1. `docker build --no-cache` — BuildKit's remote cache (inline cache embedded in previously pushed ECR images) was still being used for COPY layers in some cases, resulting in old `app.py` being copied into the image.
2. Multiple version bumps (v9 → v10 → v11 → v12) with `terraform apply` — Terraform state had drifted, reporting "No changes" even though the live task definition still referenced the old image.

**What worked:**
- Disable BuildKit entirely: `$env:DOCKER_BUILDKIT="0"; docker build -f ... -t ... .`
- Verify the fix is in the image before pushing: `docker run --rm <image> grep -n "pattern" /app/path/to/file`
- When Terraform state drifts: `terraform refresh` then `terraform apply` to re-sync state with AWS and detect image tag changes.

**Note for next time:** After any code fix, always verify with `docker run --rm <image> grep` before pushing to ECR. Do not assume `--no-cache` is sufficient when BuildKit is enabled.

---

## ECS service not picking up new image after terraform apply

**What didn't work:**
1. `aws ecs update-service --force-new-deployment` — restarts the service but uses the task definition revision the service already points to, not the latest revision in the registry.
2. `terraform apply` after tfvars image tag change — showed "No changes" due to state drift; no new task definition revision was created.

**What worked:**
1. `terraform refresh` to re-sync state with actual AWS resources.
2. `terraform apply` after refresh — detects image tag drift and creates a new task definition revision.
3. Explicitly point the service to the latest task definition revision:
   ```powershell
   $tdArn = aws ecs describe-task-definition --task-definition <family> --region ap-southeast-1 --query "taskDefinition.taskDefinitionArn" --output text
   aws ecs update-service --cluster bess-platform-cluster --service <svc> --task-definition $tdArn --force-new-deployment --region ap-southeast-1
   ```
4. Verify the running task has the correct image:
   ```powershell
   $task = aws ecs list-tasks --cluster bess-platform-cluster --service-name <svc> --region ap-southeast-1 --query "taskArns[0]" --output text
   aws ecs describe-tasks --cluster bess-platform-cluster --tasks $task --region ap-southeast-1 --query "tasks[0].containers[0].image"
   ```

**Note for next time:** After every deploy, verify the running task's image tag explicitly. `force-new-deployment` alone is not enough if the service's task definition reference is stale. Always check `terraform refresh` before concluding "No changes" is correct.

---

## Streamlit continuous page rerun (greying on/off)

**What didn't work:**
1. `time.sleep() + st.rerun()` inside tab code with a persistent session state flag (`anim_playing = True`) — all tab code runs on every Streamlit rerender regardless of which tab is active, so the animation loop fired on every render indefinitely.
2. Using `st.spinner` inside the render loop for translations — spinner triggered reruns, causing a loop when `_translate_to_zh` was called on every render.

**What worked:**
- `_anim_loop_rerun` one-shot flag: set it before `st.rerun()` in the animation loop; pop it with `.pop()` at the top of the animation init block. If the flag is absent, stop the animation. This makes the rerun self-contained and stops on any non-animation interaction.
- Translations: lazy per-item button inside expander; store result in session state; no automatic translation on render.

**Note for next time:** In Streamlit, never use `st.rerun()` in a loop controlled by a persistent session state boolean without a one-shot guard flag. All tab code runs on every render — treat it as a single flat script, not isolated tab handlers.

---

## bess-map v60 production crash — def-order NameError + _fr_df shadowing (2026-08-07)

**What happened:** v60 deployed with IRR revenue-mix feature crashed the entire app on load: `NameError: load_sysopfee is not defined`. Every user hit a traceback page.

**Root causes (both from the same loader hoist):**
1. `_sof_df = load_sysopfee(_ENG_KEY)` was hoisted to module level (line ~1733) so the geo-map tab could share it, but `def load_sysopfee` stayed at line ~2702 — Streamlit runs top-to-bottom, so the name didn't exist yet. `load_cap_comp`/`load_fr_market` defs were placed correctly; only this one was missed.
2. Latent second crash: the FR-demand section reused `_fr_df` for a localized display frame (`set_index(_t("demand_province"))`), clobbering the shared frame the aux tab reads (`_fr_df["province"]` → KeyError) — only fires when FR market data is non-empty, so it would have hit prod as soon as bug 1 was fixed.

**Why it shipped:** the 18 v60 tests only covered `irr_helpers.py`; nobody executed app.py once before deploy. Unit tests cannot catch module-level execution-order bugs.

**What worked:**
- Headless full-script execution via `streamlit.testing.v1.AppTest` (with env from config/.env) — reproduces module-level crashes locally without a browser. Now the standard pre-deploy smoke test for Streamlit apps.
- Static regression guards in `apps/bess-map/tests/test_app_def_order.py`: (a) no module-level call may precede its def; (b) shared frames `_sof_df/_cc_df/_fr_df` may only be assigned once at module level.

**Follow-up trap found during fix:** `terraform apply` for bess-map would have silently stripped 5 manually-added env vars (LINGFENG_*, OPENAI_API_KEY, DEEPSEEK_API_KEY, HERMES_URL) and reverted BEDROCK_REGION to us-east-1, because tfvars/main.tf had drifted from live (tfvars image was still v48). Reconciled into terraform config before applying. Lesson: after any out-of-band task-def edit, reconcile main.tf in the same session.

**Also:** first v61 push failed to pull on Fargate — image built on arm64 Mac without `--platform linux/amd64`. Rebuild with the flag; verify with `docker buildx imagetools inspect`.
