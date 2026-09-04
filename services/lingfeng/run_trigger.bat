@echo off
REM LingFeng on-demand trigger checker — called every 15 min by Task Scheduler.
REM Exits silently (exit 0) if no trigger is pending in DB.
REM Runs the full pipeline when Hermes sets lingfeng_trigger_run in hermes_settings.

cd /d "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform"

"C:\Users\dipeng.chen\AppData\Local\Programs\Python\Python313\python.exe" ^
    "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\services\lingfeng\run_daily.py" ^
    --markets all --models ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1 ^
    --check-trigger ^
    >> "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\logs\lingfeng_trigger.log" 2>&1
