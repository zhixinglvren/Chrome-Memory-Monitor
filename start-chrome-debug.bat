@echo off
:: Chrome 调试模式启动器
:: 用此快捷方式启动 Chrome，监控脚本可直接复用（无需重启浏览器）
:: 使用方法：双击此文件，或将其发送到桌面创建快捷方式
::
:: 重要：如果已有普通 Chrome 在运行，本脚本会使用独立用户数据目录启动，
::        确保 --remote-debugging-port 生效（否则参数会被已有进程忽略）

set CHROME_PATH="C:\Program Files\Google\Chrome\Application\chrome.exe"

if not exist %CHROME_PATH% set CHROME_PATH="C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

:: 使用独立用户数据目录，避免被已有 Chrome 实例接管导致调试端口失效
set PROFILE_DIR=%~dp0.chrome-debug-profile

echo Chrome 调试模式已启动（端口 9222），本窗口 3 秒后自动关闭...

start "" %CHROME_PATH% --remote-debugging-port=9222 --user-data-dir="%PROFILE_DIR%" --enable-precise-memory-info

timeout /t 3 /nobreak >nul
