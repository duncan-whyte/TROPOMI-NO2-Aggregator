import argparse
from functools import partial
from os import makedirs
import xarray as xr
import rioxarray
from pathlib import Path
from tqdm import tqdm
from os.path import exists
import datetime
from s5p_tools import (
    compute_lengths_and_offsets,
    fetch_product,
    generate_harp_commands,
    preprocess_time,
    process_file,
    DHUS_USER,
    DHUS_PASSWORD,
    DHUS_URL,
    DOWNLOAD_DIR,
    EXPORT_DIR,
    PROCESSED_DIR
)

def processL3(filenames, chunk_size):
    # Recover attributes
    print(filenames[0])
    attributes = {}
    for filename in filenames:
        rxds=rioxarray.open_rasterio(filename)
        attributes[str(filename.name).replace("L3","L2")] = {
            "time_coverage_start": rxds.attrs["time_coverage_start"],
            "time_coverage_end": rxds.attrs["time_coverage_end"],
        }
        rxds.close()
    tqdm.write("Processing data\n")
    xr.set_options(keep_attrs=True)

    DS = xr.open_mfdataset(
        [
            str(filename.relative_to(".")).replace("L2", "L3")
            for filename in filenames
            if exists(str(filename.relative_to(".")).replace("L2", "L3"))
        ],
        combine="nested",
        concat_dim="time",
        parallel=True,
        preprocess=partial(
            preprocess_time,
            attributes=attributes
            ),
        decode_times=False,
        chunks={"time": chunk_size},
    )[['cloud_fraction', 'tropospheric_NO2_column_number_density']] # todo remove
    DS = DS.sortby("time")
    DS.rio.write_crs("epsg:4326", inplace=True)
    DS.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)

    tqdm.write("Exporting netCDF file\n")
    start = datetime.datetime.fromtimestamp(min(DS['time']).item()//1e9)
    end = datetime.datetime.fromtimestamp(max(DS['time']).item()//1e9)
    
    makedirs(PROCESSED_DIR, exist_ok=True)
    file_export_name = PROCESSED_DIR / (
        f"{start.day}-{start.month}-{start.year}__"
        f"{end.day}-{end.month}-{end.year}__{filenames[0].name[:-3]}.nc"
    )
    
    DS.to_netcdf(file_export_name)

    print("Done!")
    return DS


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=(
            "Request, download and process Sentinel data from Copernicus access hub. "
            "Create a processed netCDF file binned by time, latitude and longitude"
        )
    )

    # chunk-size:
    parser.add_argument(
        "--chunk-size", "-c",
        help="dask chunk size along the time dimension",
        type=int,
        default=256,
    )
    
    parser.add_argument(
        "filenames",
        help="Names of the files to process",
        nargs='+'
    )
    
    args = parser.parse_args()
    #print(len(args.filenames))
    processL3([Path(x) for x in args.filenames], args.chunk_size)