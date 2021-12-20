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
from shapely.geometry import mapping
import datetime

xr.set_options(keep_attrs=True)

if __name__ == '__main__': # main
    
    # Neem input
    
    parser = argparse.ArgumentParser(description="Analyse")
    
    # .nc met de satellietdata
    parser.add_argument("raster", help="Satellite data file", type=str)
    # .shp met de grenzen van het land
    parser.add_argument("shapefile", help="Topographic shape file", type=str)
    
    args = parser.parse_args()
    
    # Laad topografie met de EPSG:4326 standaard (coordinaten)
    geodf = geopandas.read_file(args.shapefile, crs="EPSG:4326")
    # Laad satellietdata
    xds = rioxarray.open_rasterio(args.raster)
    #print(xds)
    # Neem alleen de waardes binnen het land, de rest wordt een bepaalde constante >10^30
    clipped = xds.rio.write_crs('EPSG:4326').rio.clip(geodf.geometry.apply(mapping), geodf.crs, drop=False, invert=True)
    #print(clipped)
    
    #clipped.to_netcdf('clipped.nc')
    
    for t in range(clipped['time'].size):
    
        # Neem alle waardes behalve die die gelijk zijn aan de constante
        whered = clipped[{'time':t}].where(clipped<1e+30)
        
        # Waardes om op te slaan:
        # Gemiddeldes
        meaned = whered.mean()
        cloudfrac = meaned['cloud_fraction'].item()
        no2 = meaned['tropospheric_NO2_column_number_density'].item()
        # Aantal vakjes binnen het land
        counted = whered.count()['cloud_fraction'].item()
        # Aantal vakjes totaal
        origcounted = clipped[{'time':t}].count()['cloud_fraction'].item()
        # Meettijd/processingtijd
        creation = clipped[{'time':t}].time.item().isoformat()
        # Tijdstip klaar met analyse, i.e. nu
        timeanalysisdone = datetime.datetime.now().isoformat()
        # Naam van het satellietdatabestand
        rastername = args.raster
        # Naam van het topografiebestand (met landnaam erin)
        shapefilename = args.shapefile
        
        print(rastername,shapefilename,creation,timeanalysisdone,counted,origcounted,cloudfrac,no2)
        #f.write("rastername,shapefilename,creation,timeanalysisdone,counted,origcounted,cloudfrac,no2\n")
        with open('analysis.csv','a') as f:
            f.write(f'{rastername},{shapefilename},{creation},{timeanalysisdone},{counted},{origcounted},{cloudfrac},{no2}\n')
    
