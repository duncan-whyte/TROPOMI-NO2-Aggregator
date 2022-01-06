# TROPOMI-NO2-Aggregator

This repo includes three separate processes:

#### `request.py`
`request.py` retrieves into `L2_data/` and reduces into `L3_data/` the resolution of all `.nc` files covering a certain area (`aoi`, as `.geojson`) in a certain
timeframe (`--date yyyyMMdd yyyyMMdd`). With the `-l`/`--links` flag the program quits after printing the URLs. The `-s`/`--skip` flag starts reducing already
downloaded files without downloading missing files. Part of this code is courtesy of Bilel Omrani's [`s5p-analysis`](https://github.com/bilelomrani1/s5p-analysis), under an MIT license found in this file.


#### `process.py`
After that, `process.py` takes in those files as argument and averages them over time, saving them in `processed/`.

#### `analyse.py`
`analyse.py` clips a processed file (`raster`) to a certain area's shape (`shapefile`, as `.shp`, see `shp/` folder), averaging them over the surface and registering the result
and other information about the file in `analysis.csv` (no flag) or `fnanalysis.csv` (`-s`).

#### Requirements

    harp
    numpy
    pandas
    sentinelsat
    tqdm
    geopandas
    xarray
    rioxarray
    netCDF4
    shapely

