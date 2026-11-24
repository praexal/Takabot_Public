@echo off
:: Ensure no old flag file is left behind before starting masterbot
if exist master_ready.flag del master_ready.flag

echo.
echo Login successful! Starting Master Bot...
start py masterbotauto.py

echo Waiting for Master Bot to finish seeding historical data...

:: Loop and wait until the flag file is created by masterbotauto.py
:WAIT_LOOP
timeout /t 2 /nobreak >nul
if not exist master_ready.flag (
    goto WAIT_LOOP
)

echo.
echo Master Bot seeding complete! Starting trading scripts...

start py TV.py
start py OB.py

timeout /t 10 /nobreak

start py poc3slave.py
start py obslave.py
start py orb.py
start py datalogger.py