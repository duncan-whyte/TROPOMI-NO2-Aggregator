cd ../
call conda activate pws2
python process.py %*
call conda deactivate
pause