@echo off
title Kripto Futures Analiz Sistemi
color 0A
cls
echo =============================================
echo   KRIPTO PARA FUTURES PIYASA ANALIZ SISTEMI
echo =============================================
echo.
echo 1- Backend sunucusunu baslat (Python API)
echo 2- Frontend arayuzunu ac (index.html)
echo 3- Tumunu baslat
echo 4- Cikis
echo.

set /p secim=Seciminiz (1-4): 

if "%secim%"=="1" goto start_backend
if "%secim%"=="2" goto start_frontend
if "%secim%"=="3" goto start_all
if "%secim%"=="4" goto exit
goto end

:start_backend
cls
echo Backend sunucusu baslatiliyor...
echo.
cd crypto-trader
start /B python backend.py
echo.
echo Backend sunucusu http://localhost:5000 adresinde calisiyor
echo.
echo Ana sayfa: http://localhost:5000
pause
goto end

:start_frontend
cls
echo Frontend arayuzu aciliyor...
cd crypto-trader
start index.html
goto end

:start_all
cls
echo Backend ve Frontend baslatiliyor...
echo.
cd crypto-trader
start /B python backend.py
timeout /t 3 >nul
start index.html
echo.
echo Backend: http://localhost:5000
echo Frontend: index.html (tarayicida acildi)
echo.
pause
goto end

:exit
exit

:end
