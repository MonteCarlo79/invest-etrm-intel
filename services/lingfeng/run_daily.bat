@echo off
REM LingFeng daily collection wrapper — called by Windows Task Scheduler.
REM Using a .bat avoids the cmd.exe /c quoting issue where multiple quoted
REM arguments on the same line get the outer quotes stripped.

cd /d "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform"

"C:\Users\dipeng.chen\AppData\Local\Programs\Python\Python313\python.exe" ^
    "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\services\lingfeng\run_daily.py" ^
    --markets all --models ols_rt_time_v1,naive_rt_ar17,ols_fundamentals_v1 ^
    >> "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\logs\lingfeng_daily.log" 2>&1
