# How to download the zarr file

The dataset comes from copy of cloud optimized data, using query like this and execute in the
corresponding ipnb in https://github.com/aodn/aodn_cloud_optimised/tree/main/notebooks

```python
# These two dataset do not have depth
ds1 = aodn_dataset.get_data(date_start='2024-02-04', date_end='2024-02-06')
ds1.to_zarr('/tmp/vessel_satellite_radiance_delayed_qc.zarr', mode='w')

ds1 = aodn_dataset.get_data(date_start='2024-02-01', date_end='2024-02-06')
ds1.to_zarr('/tmp/vessel_satellite_radiance_derived_product.zarr', mode='w')

ds1 = aodn_dataset.get_data(date_start='2011-11-16', date_end='2011-11-19')
ds1.to_zarr('/tmp/satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr', mode='w')

# This one should have depth
```
