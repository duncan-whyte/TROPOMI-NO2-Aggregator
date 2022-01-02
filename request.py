# Modified, courtesy of https://github.com/bilelomrani1/s5p-analysis
# under following license:
#
# MIT License
#
# Copyright (c) 2019 Bilel Omrani
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import argparse
from functools import partial
import geopandas
from multiprocessing.pool import ThreadPool, Pool
from os import cpu_count, makedirs
from os.path import exists
from pathlib import Path
import rioxarray
from sentinelsat.sentinel import (
    SentinelAPI,
    geojson_to_wkt,
    read_geojson,
)
import sys
from tqdm import tqdm
import xarray as xr
import netCDF4 as nc
import urllib.parse

from utils import (
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
    PROCESSED_DIR,
    HARPED_DIR
)


def main(
    producttype="L2__NO2___",
    aoi="nl.geojson",
    date=("NOW-24HOURS","NOW"),
    qa=50,
    unit="mol/m2",
    resolution=(0.1,0.1),
    num_threads=4,
    num_workers=cpu_count(),
    links=False,
    skip=False
):

    api = SentinelAPI(DHUS_USER, DHUS_PASSWORD, DHUS_URL)

    tqdm.write("\nRequesting products\n")

    query_body = {
        "date": date,
        "platformname": "Sentinel-5 Precursor",
        "producttype": producttype
    }

    # query database
    if aoi is None:
        products = api.query(**query_body)
    else:
        footprint = geojson_to_wkt(read_geojson(Path(aoi)))
        products = api.query(footprint, **query_body)

    # display results
    tqdm.write(
        (
            "Number of products found: {number_product}\n"
            "Total products size: {size:.2f} GB\n"
        ).format(
            number_product=len(products),
            size=api.get_products_size(products)
            )
    )
    for uuid in products.keys():
        tqdm.write(products[uuid]["link"].replace("'",'%27'))
    if links:
        return
    

    # list of uuids for each product in the query
    ids_request = list(products.keys())

    if len(ids_request) == 0:
        tqdm.write("Done!")
        return

    # list of downloaded filenames urls

    makedirs(DOWNLOAD_DIR, exist_ok=True)

    #with ThreadPool(num_threads) as pool:
    #    pool.map(
    #        partial(
    #            fetch_product,
    #            api=api,
    #            products=products,
    #            download_dir=DOWNLOAD_DIR,
    #            skip=skip
    #        ),
    #        ids_request)
    #
    #    pool.close()
    #    pool.join()
    
    filenames = [
        DOWNLOAD_DIR / f"{products[file_id]['title']}.nc"
        for file_id in ids_request if fetch_product(file_id, api, products, download_dir=DOWNLOAD_DIR, skip=skip)
    ]
    convertL3(
        producttype,
        aoi,
        date,
        qa,
        unit,
        resolution,
        num_threads,
        num_workers,
        filenames
    )

def convertL3(
    producttype="L2__NO2___",
    aoi="nl.geojson",
    date=("NOW-24HOURS","NOW"),
    qa=50,
    unit="mol/m2",
    resolution=(0.1,0.1),
    num_threads=4,
    num_workers=cpu_count(),
    filenames=[]
    ):

    tqdm.write("Converting into L3 products\n")

    # Step size for spatial re-gridding (in degrees)
    xstep, ystep = resolution

    if aoi is None:
        minx, miny, maxx, maxy = -180, -90, 180, 90
    else:
        minx, miny, maxx, maxy = geopandas.read_file(Path(aoi)).bounds.values.squeeze()

    # computes offsets and number of samples
    lat_length, lat_offset, lon_length, lon_offset = compute_lengths_and_offsets(minx, miny, maxx, maxy, ystep, xstep)

    harp_commands = generate_harp_commands(
        producttype,
        qa,
        unit,
        xstep,
        ystep,
        lat_length,
        lat_offset,
        lon_length,
        lon_offset
    )

    makedirs(EXPORT_DIR, exist_ok=True)
    makedirs(HARPED_DIR, exist_ok=True)
    

    """
    tqdm.write(f"Launched {num_workers} processes")
    with Pool(processes=num_workers) as pool:
        list(
            tqdm(
                pool.imap_unordered(
                    partial(
                        process_file,
                        harp_commands=harp_commands,
                        export_dir=EXPORT_DIR
                    ),
                    filenames,
                ),
                desc="Converting",
                leave=False,
                total=len(filenames),
            )
        )
        pool.close()
        pool.join()
    """
    donefiles = []
    for f in filenames:
        tqdm.write(str(f))
        try:
            export_url = process_file(f, harp_commands, export_dir=HARPED_DIR)
            donefiles.append(f)
        except Exception as e:
            print(e)
            #raise e
            sys.stderr.write(f'Error in {str(f)}\n')
            continue
    #processL3(donefiles, producttype, chunk_size)
    
    #for f in donefiles:
    #    str(filename.relative_to(".")).replace("L2", "L3")
    
    for f in donefiles:
        xds = xr.open_dataset(f)
        attrs = {
            "time_coverage_start": xds.attrs["time_coverage_start"],
            "time_coverage_end": xds.attrs["time_coverage_end"],
        }
        xds.close()
        xds = xr.open_dataset(HARPED_DIR / f.name.replace("L2", "L3"))
        xds.attrs.update(attrs)
        xds.close()
        xds.to_netcdf(EXPORT_DIR / f.name.replace("L2", "L3"))
    
    tqdm.write('\n\n\nFilenames:\n')
    tqdm.write(' '.join([str(x) for x in donefiles])+'\n')
    # todo delete donefiles irl


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description=(
            "Request and download Sentinel data from Copernicus access hub. "
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
    parser.add_argument("--product", "-p", help="product type", type=str, default="L2__NO2___")

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
        "--date", "-d",
        help="date used to perform a time interval search",
        nargs=2,
        type=str,
        default=("NOW-24HOURS", "NOW"),
    )
    
    parser.add_argument(
        "--links", "-l",
        help="only print links",
        action='store_true'
    )
    parser.add_argument(
        "--skip", "-s",
        help="skip download",
        action='store_true'
    )

    # Area of interest: The url of the area of interest (.geojson)
    parser.add_argument(
        "aoi", help="Path to the area of interest (.geojson)", type=str
    )

    # Unit: Unit conversion
    parser.add_argument("--unit", "-u", help="unit conversion", type=str, default="mol/m2")

    # qa value: Quality value threshold
    parser.add_argument("--qa", "-q", help="quality value threshold", type=int, default=50)

    # resolution: Spatial resolution in arc degrees
    parser.add_argument(
        "--resolution", "-r",
        help="spatial resolution in arc degrees",
        nargs=2,
        type=float,
        default=(0.1, 0.1),
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

    main(
        producttype=args.product,
        aoi=args.aoi,
        date=args.date,
        qa=args.qa,
        unit=args.unit,
        resolution=args.resolution,
        num_threads=args.num_threads,
        num_workers=args.num_workers,
        links=args.links,
        skip=args.skip
    )
