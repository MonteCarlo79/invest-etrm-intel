Before starting this task, apply the Karpathy guidelines to structure your approach:

## 1. Think Before Coding

State your assumptions explicitly right now:
- What is the task asking for?
- Are there multiple valid interpretations? If yes, list them and confirm before proceeding.
- Is there a simpler approach than what was asked? If yes, say so.
- What is genuinely unclear? Name it. Ask.

Do not start writing code until assumptions are stated.

## 2. Simplicity First

Commit to minimum viable implementation:
- No features beyond what was asked
- No abstractions for single-use code (three similar lines > premature abstraction)
- No error handling for impossible scenarios (trust framework guarantees)
- No "flexibility" or "configurability" that wasn't requested
- Python/Streamlit specific: no extra `st.expander`, no extra tabs, no extra columns unless asked

## 3. Surgical Changes

Before touching anything, list the exact files and functions you will change:
- Every changed line must trace to the user's request
- Do not "improve" adjacent Streamlit components, SQL queries, or formatting
- Do not refactor working code you didn't break
- If your changes create unused imports/variables, remove them
- If you notice unrelated dead code, mention it in the follow-up section — don't delete it

## 4. Goal-Driven Execution

Transform the task into a verifiable success criterion:

| Task type | Success criterion |
|-----------|-------------------|
| Bug fix | Test/check that reproduces the bug now passes |
| New tab/feature | Renders locally at correct port with real DB data |
| DB schema change | Migration runs idempotently; existing queries unbroken |
| Refactor | All existing tests pass before and after |
| Deploy | ECS service shows RUNNING; ALB health check passes |

State your success criterion now, before writing any code. Then loop until it is met.

---

**Output format after applying these guidelines:**

```
Assumptions: [list]
Interpretation chosen: [which one and why, or "confirmed — only one interpretation"]
Files to change: [exact paths]
Success criterion: [verifiable check]
Plan:
1. [step] → verify: [check]
2. [step] → verify: [check]
```
