"""Make an xarray dataset safe to serialise as NetCDF."""

import numpy as np
import xarray
import dask.array as da


def convert_object_dtype_variables(dataset: xarray.Dataset, logger) -> xarray.Dataset:
    """
    Convert object dtype variables to fixed-size string dtype by:
    1. Loading data in chunks to determine max string length (memory-safe)
    2. Converting to appropriate fixed-size dtype (e.g., S64, S128)

    This avoids the SerializationWarning and prevents sudden memory allocation
    when saving to NetCDF.
    """

    for var_name in list(dataset.variables.keys()):
        var = dataset[var_name]

        # Check if variable has object dtype
        if var.dtype != np.dtype("object"):
            continue

        logger.info(f"Processing object dtype variable: {var_name}")
        logger.info(f"  Shape: {var.shape}, Size: {var.size}")

        # Determine max string length by processing in chunks
        max_length = 0

        if isinstance(var.data, da.Array):
            # Dask array - process chunk by chunk
            logger.info(
                f"  Finding max length for var: {var_name} using Dask chunks..."
            )

            # Process each chunk to find max length
            for chunk_idx in range(
                var.data.npartitions if hasattr(var.data, "npartitions") else 1
            ):
                try:
                    # Get chunk and compute it
                    chunk = (
                        var.data.blocks[chunk_idx]
                        if hasattr(var.data, "blocks")
                        else var.data
                    )
                    chunk_data = chunk.compute()

                    # Find max length in this chunk
                    chunk_lengths = [
                        len(str(item)) if item is not None else 0
                        for item in chunk_data.flat
                    ]
                    chunk_max = max(chunk_lengths) if chunk_lengths else 0
                    max_length = max(max_length, chunk_max)

                except Exception as e:
                    logger.warning(f"    Error processing chunk {chunk_idx}: {e}")
                    # If we can't process chunks individually, fall back to computing all
                    break

            # If chunk processing failed or max_length is still 0, compute the whole array
            if max_length == 0:
                logger.info(f"  Computing entire array to find max length...")
                computed_data = var.compute()
                all_lengths = [
                    len(str(item)) if item is not None else 0
                    for item in computed_data.values.flat
                ]
                max_length = max(all_lengths) if all_lengths else 0
        else:
            # Already a numpy array - process directly
            logger.info(f"  Processing numpy array...")
            all_lengths = [
                len(str(item)) if item is not None else 0 for item in var.values.flat
            ]
            max_length = max(all_lengths) if all_lengths else 0

        # Add buffer to max_length (20% extra or at least 16 bytes)
        safe_length = max(16, int(max_length * 1.2))

        # Choose appropriate dtype based on length
        if safe_length <= 32:
            dtype = "S32"
        elif safe_length <= 64:
            dtype = "S64"
        elif safe_length <= 128:
            dtype = "S128"
        elif safe_length <= 256:
            dtype = "S256"
        else:
            logger.warning(
                f"  Very long strings detected in variable {var_name} (length: {safe_length}). "
                f"Using dtype S{safe_length}, which may increase file size."
            )
            dtype = f"S{safe_length}"

        logger.info(f"  Max length found: {max_length}, using dtype: {dtype}")

        # Convert the variable to fixed-size string
        dataset[var_name] = var.astype(dtype)
        logger.info(f"  Converted {var_name} to {dtype}")

    return dataset


def ignore_invalid_unicode_in_attrs(dataset: xarray.Dataset) -> xarray.Dataset:
    for k, v in dataset.attrs.items():
        if isinstance(v, str):
            dataset.attrs[k] = v.encode("utf-8", errors="ignore").decode("utf-8")

    return dataset
