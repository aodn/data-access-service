import sys

import xarray as xr
import fsspec
import asyncio

dataset_name = "satellite_diffuse_attenuation_coefficent_1day_noaa20"


async def main():
    # Set the event loop policy to use the default event loop
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

    url = f's3://aodn-cloud-optimised/{dataset_name}.zarr/'
    ds = xr.open_zarr(fsspec.get_mapper(url, anon=True), chunks=None, consolidated=True)

    ds_size = ds.nbytes / (1024 ** 3)  # Size in GB
    print(f"Dataset size: {ds_size:.2f} GB")
    ds_instance_size = sys.getsizeof(ds)

    # size in b
    print(f"Dataset instance size: {ds_instance_size} bytes")

    # Define bbox and date range
    lat_min, lat_max = -60, -59  # Latitude range
    lon_min, lon_max = 120, 121  # Longitude range
    start_date, end_date = "2023-03-01", "2023-03-31"  # Date range

    # Query data
    subset = ds.sel(
        latitude=slice(lat_max, lat_min),  # Use slice for descending order
        longitude=slice(lon_min, lon_max),
        time=slice(start_date, end_date)
    )

    print(subset)

    whole_set = ds.sel()
    print("whole set", whole_set)


    # output_file = "subset_output.nc"
    # subset.to_netcdf(output_file)
    # print(f"Subset saved to {output_file}")

    # specific_lat = -59.5
    # specific_lon = 120.5
    # specific_time = "2023-02-08T12:00:00"
    #
    # # Query the specific data point
    # data_point = ds.sel(
    #     latitude=specific_lat,
    #     longitude=specific_lon,
    #     time=specific_time,
    #     method="nearest"  # Optional: Use "nearest" to get the closest match
    # )
    #
    # print("data point")
    # print(data_point)


if __name__ == '__main__':
    asyncio.run(main())
