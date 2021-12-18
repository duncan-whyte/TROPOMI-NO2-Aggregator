import xarray as xr
import numpy as np
import pandas as pd
from itertools import product, cycle
import argparse
from pathlib import Path


xr.set_options(keep_attrs=True)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Nvm")
    parser.add_argument("raster", help="path to the a raster tif file", type=str)
    args = parser.parse_args()
    rasterpath = Path(args.raster)
else:
    rasterpath = Path("L3_data\\S5P_NRTI_L3__NO2____20211217T114745_20211217T115245_21651_02_020301_20211217T131229.nc")

data = xr.open_dataset(rasterpath) #.resample(time='1D').mean(dim='time')
#tncnd = data['tropospheric_NO2_column_number_density']
