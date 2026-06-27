@echo off
cd /d "%~dp0"
title Chrome 内存监控

echo.
echo ========================================
echo   Chrome 内存监控 (Python+CDP)
echo   实时内存+DOM+渲染 自动分配采样、堆快照
echo ========================================
echo.

where python >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到 Python
    pause
    exit /b 1
)

python -c "import websockets" >nul 2>&1
if errorlevel 1 (
    echo [安装] 正在安装 websockets...
    python -m pip install websockets --quiet
    if errorlevel 1 (
        echo [错误] websockets 安装失败
        pause
        exit /b 1
    )
)

echo.
echo [启动] 正在启动监控...
echo.
python chrome-memory-monitor.py
echo.
echo 监控已结束
pause
