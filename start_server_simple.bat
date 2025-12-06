@echo off
chcp 65001 >nul
title Threads Collector API Server
echo ========================================
echo Threads Collector API 서버
echo ========================================
echo.
echo 포트: 8001
echo API 문서: http://localhost:8001/docs
echo.
echo 서버 시작 중...
echo.
cd /d "%~dp0"
uv run python run_server.py
pause

