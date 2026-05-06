# ERRORS.md — bess-platform

Check this before suggesting approaches to tasks similar to those logged below. If a match is found, skip to what worked.

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
