# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 15:58:02 2024

@author: Anthony.R.Klemm
"""

import shutil
import bagPy as BAG
import numpy as np
import h5py
import pathlib
from osgeo import gdal
import json

# Define file paths
INPUTS = pathlib.Path(__file__).parents[1] / 'inputs'
OUTPUTS = pathlib.Path(__file__).parents[1] / 'outputs'
input_bag_path = str(INPUTS / "H13667_MB_VR_Ellipsoid_1of1.bag")
output_bag_path = str(OUTPUTS / "H13667_MB_VR_Ellipsoid_1of1_v2.0_OCS_200.bag")


# Copy and rename the BAG file
shutil.copy(input_bag_path, output_bag_path)
print(f"Copied and renamed the BAG file to: {output_bag_path}")

# Open the copied BAG file with gdal to extract elevation data
open_options = ['MODE=LIST_SUPERGRIDS']
# ds = gdal.OpenEx(output_bag_path, gdal.GA_ReadOnly, open_options=open_options)
# print(dir(ds))
# layer_count = ds.GetLayerCount()
# print('layer count:', layer_count)

# for i in range(0, layer_count):
#     layer = ds.GetLayerByIndex(i)
#     print('layer:', layer)
#     print(dir(layer))
#     schema = layer.schema

#     for item in schema:
#         print('item:', item.GetName())
#         print(dir(item))

ds = gdal.Open(output_bag_path, gdal.GA_ReadOnly)


elevation_band = ds.GetRasterBand(1)
print(elevation_band)
elevation_data = elevation_band.ReadAsArray()
print(elevation_data)

# Create a boolean mask from the elevation data
no_data_value = elevation_band.GetNoDataValue()
mask = (elevation_data != no_data_value)

# Close GDAL dataset for later copying
ds = None

# Open the copied BAG file
try:
    dataset = BAG.Dataset.openDataset(output_bag_path, BAG.BAG_OPEN_READ_WRITE)
    descriptor = dataset.getDescriptor()
    print('version:', descriptor.getVersion())
    descriptor.setVersion('2.0.1')
    print('version:', descriptor.getVersion())
    #close', 'create', 'createGeorefMetadataLayer', 'createMetadataProfileGeorefMetadataLayer', 'createSimpleLayer', 'createSurfaceCorrections', 
    # 'createVR', 'geoToGrid', 'getDescriptor', 'getGeorefMetadataLayer', 'getGeorefMetadataLayers', 'getLayer', 'getLayerTypes', 'getLayers', 
    # getMetadata', 'getSimpleLayer', 'getSurfaceCorrections', 'getTrackingList', 'getVRMetadata', 'getVRNode', 'getVRRefinements', 
    # 'getVRTrackingList', 'gridToGeo', 'openDataset', 'this', 'thisown

    print("Dataset opened successfully.")
except Exception as e:
    print(f"Error opening dataset: {e}")
    raise

# Add georeferenced metadata layer using the NOAA_OCS_2022_10 Metadata definition
try:
    indexType = BAG.DT_UINT16
    chunkSize = 100
    compressionLevel = 6

    definition = BAG.METADATA_DEFINITION_NOAA_OCS_2022_10

    # TODO Use "Elevation" as third argument, then change it using h5py later - I think this is a bug
    georefMetaLayer = dataset.createGeorefMetadataLayer(
        indexType, 
        BAG.NOAA_OCS_2022_10_METADATA_PROFILE, 
        "Elevation", 
        definition, 
        chunkSize, 
        compressionLevel)
    print("Georeferenced metadata layer added successfully.")

    # Add records using the NOAA OCS 2022-10 template
    record1 = BAG.CreateRecord_NOAA_OCS_2022_10(True, True, 2.0, 0.05, True, False, 5.0, 0.05, "2011-02-10", "2011-06-29",
                                                "NOAA Office of Coast Survey", "H12286", 1, "CC0-1.0", "https://creativecommons.org/publicdomain/zero/1.0/")
    valueTable = georefMetaLayer.getValueTable()
    firstRecordIndex = valueTable.addRecord(record1)
    print("NOAA OCS 2022-10 records added successfully.")

except Exception as e:
    print(f"Error adding georeferenced metadata layer: {e}")
finally:
    dataset.close()

# Update the key layer using h5py
with h5py.File(output_bag_path, 'a') as f:
    try:
        f.move('/BAG_root/georef_metadata/Elevation', '/BAG_root/georef_metadata/NOAA_OCS_2022_10')
        print("Group renamed successfully.")
    except Exception as e:
        print(f"Error renaming group: {e}")

    try:
        # Create the keys dataset
        numRows, numColumns = elevation_data.shape
        if '/BAG_root/georef_metadata/NOAA_OCS_2022_10/keys' in f:
            del f['/BAG_root/georef_metadata/NOAA_OCS_2022_10/keys']
        keys_ds = f.create_dataset('/BAG_root/georef_metadata/NOAA_OCS_2022_10/keys', (numRows, numColumns), dtype=np.uint16)
        
        # Apply the mask to create the key layer and flip it vertically
        keys = np.zeros((numRows, numColumns), dtype=np.uint16)
        keys[mask] = 1
        keys = np.flipud(keys)  # Flip the array vertically
        keys_ds[...] = keys
        print("Key layer added successfully.")
    except Exception as e:
        print(f"Error creating key layer: {e}")

print("Georeferenced metadata group renamed and key layer added successfully.")

dataset = None
output_hdf = str(OUTPUTS / "H13667_MB_VR_Ellipsoid_1of1_v2_for_viewing.hdf")
shutil.copyfile(str(OUTPUTS / "H13667_MB_VR_Ellipsoid_1of1_v2.0_OCS_200.bag"), output_hdf)