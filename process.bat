call conda activate pws2
for /f "tokens=*" %%G in ('dir /b "L3_data"') do python process.py "L3_data\%%G"
call conda deactivate
pause