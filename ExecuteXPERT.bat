cd /d %~dp0

:: run script_file with params from csv file
for /f "skip=1 tokens=1,2,3,4 delims=," %%a in (input.csv) do (
    start /wait %cd%\main.exe -i %cd%\req.csv -n %cd%\filesToSkip.json -v %%a -m %%b -f %%c --versions %%d >> XPERT.log 2>&1
)

:: don't auto exit script
pause
