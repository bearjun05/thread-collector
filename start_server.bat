@echo off
chcp 65001 >nul
echo Threads Collector API 서버 시작 중...
echo.
cd /d "%~dp0"
uv run uvicorn app:app --host 0.0.0.0 --port 8000 --reload
pause

