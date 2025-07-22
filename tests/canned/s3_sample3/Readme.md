ds1 = aodn_dataset.get_data(date_start='2008-08-01', date_end='2008-09-01')
ds1.to_zarr('/tmp/satellite_ghrsst_l4_gamssa_1day_multi_sensor_world.zarr', mode='w')
