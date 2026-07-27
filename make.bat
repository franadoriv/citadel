@echo off
rem Citadel Windows task runner forwarder.
rem
rem Lets `make <target> [options]` work from cmd, and `.\make <target>` work
rem from PowerShell, without typing `.\make.ps1`. All arguments (the target
rem name plus any named options such as -WebPort 9000) are forwarded verbatim
rem to make.ps1, which does the real work. CI keeps calling make.ps1 directly.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0make.ps1" %*
exit /b %ERRORLEVEL%
