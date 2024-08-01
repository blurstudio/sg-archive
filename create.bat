@echo off
cd %~dp0

echo -- Arguments: %*

:: Create the virtualenv if it doesn't exist and install requirements
if not exist venv (
    echo -- Creating venv
    py %* -m virtualenv venv

    echo -- Installing Requirements
    venv\Scripts\pip install -r requirements.txt
)

echo.
echo -- The command prompt has been configured to run the download script.
echo -- Use the command `python download.py --help` to get info on commands to run.

:: Activate the virtualenv
venv\Scripts\activate
