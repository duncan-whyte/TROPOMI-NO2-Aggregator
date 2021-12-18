import xarray as xr
import numpy as np
import pandas as pd
from itertools import product, cycle
import argparse
from pathlib import Path
import s5prequest as s5pr
from os import cpu_count
import rioxarray
import geopandas

xr.set_options(keep_attrs=True)

#if __name__=="__main__":
#    parser = argparse.ArgumentParser(description="Nvm")
#    parser.add_argument("raster", help="path to the a raster tif file", type=str)
#    args = parser.parse_args()
#    rasterpath = Path(args.raster)
#else:
#    rasterpath = Path("L3_data\\S5P_NRTI_L3__NO2____20211218T112745_20211218T113245_21665_02_020301_20211218T124629.nc")

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Analyse")
    
    parser.add_argument("filename", help="File name", type=str)
    
    geodf = geopandas.read_file()
    
    value = data.mean(dim='longitude').mean(dim='latitude'))
    print(value)
