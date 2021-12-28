if [%1]==[] (set shapefile="shp\nl_10km.shp") else (set shapefile="%1")
call conda activate pws2
for /f "tokens=*" %%G in ('dir /b "processed"') do python analyse.py "processed\%%G" %shapefile%
call conda deactivate
pause