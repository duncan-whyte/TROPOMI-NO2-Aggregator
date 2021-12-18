import xarray as xr
import numpy as np
import pandas as pd
from itertools import product, cycle
import argparse
from pathlib import Path
import s5p-request as s5pr


xr.set_options(keep_attrs=True)

#if __name__=="__main__":
#    parser = argparse.ArgumentParser(description="Nvm")
#    parser.add_argument("raster", help="path to the a raster tif file", type=str)
#    args = parser.parse_args()
#    rasterpath = Path(args.raster)
#else:
#    rasterpath = Path("L3_data\\S5P_NRTI_L3__NO2____20211218T112745_20211218T113245_21665_02_020301_20211218T124629.nc")

print(value)

if __name__ == '__main__':
    if True:
        parser = argparse.ArgumentParser(
            description=(
                "Request, download and process Sentinel data from Copernicus access hub. "
                "Create a processed netCDF file binned by time, latitude and longitude"
            )
        )

        # Product type: Used to perform a product based search
        # Possible values are
        #   L2__O3____
        #   L2__NO2___
        #   L2__SO2___
        #   L2__CO____
        #   L2__CH4___
        #   L2__HCHO__
        #   L2__AER_AI
        #   L2__CLOUD_
        parser.add_argument("product", help="Product type", type=str)

        # Date: Used to perform a time interval search
        # The general form to be used is:
        #       date=(<timestamp>, <timestamp>)
        # where < timestamp > can be expressed in one of the following formats:
        #   yyyyMMdd
        #   yyyy-MM-ddThh:mm:ssZ
        #   yyyy-MM-ddThh:mm:ss.SSSZ(ISO8601 format)
        #   NOW
        #   NOW-<n>MINUTE(S)
        #   NOW-<n>HOUR(S)
        #   NOW-<n>DAY(S)
        #   NOW-<n>MONTH(S)
        parser.add_argument(
            "--date",
            help="date used to perform a time interval search",
            nargs=2,
            type=str,
            default=("NOW-24HOURS", "NOW"),
        )

        # Area of interest: The url of the area of interest (.geojson)
        parser.add_argument(
            "--aoi", help="path to the area of interest (.geojson)", type=str
        )

        # Unit: Unit conversion
        parser.add_argument("--unit", help="unit conversion", type=str, default="mol/m2")

        # qa value: Quality value threshold
        parser.add_argument("--qa", help="quality value threshold", type=int, default=50)

        # resolution: Spatial resolution in arc degrees
        parser.add_argument(
            "--resolution",
            help="spatial resolution in arc degrees",
            nargs=2,
            type=float,
            default=(0.01, 0.01),
        )

        # chunk-size:
        parser.add_argument(
            "--chunk-size",
            help="dask chunk size along the time dimension",
            type=int,
            default=256,
        )

        # num-threads:
        parser.add_argument(
            "--num-threads",
            help="number of threads spawned for L2 download",
            type=int,
            default=4,
        )

        # num-workers:
        parser.add_argument(
            "--num-workers",
            help="number of workers spawned for L3 conversion",
            type=int,
            default=cpu_count(),
        )

        args = parser.parse_args()

    data = s5pr.main(
        producttype=args.product,
        aoi=args.aoi,
        date=args.date,
        qa=args.qa,
        unit=args.unit,
        resolution=args.resolution,
        chunk_size=args.chunk_size,
        num_threads=args.num_threads,
        num_workers=args.num_workers,
    )
    value = data.mean(dim='longitude').mean(dim='latitude'))
