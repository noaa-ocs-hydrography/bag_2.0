# -*- coding: utf-8 -*-
"""
BAG Conversion Tool - converts multiple BAG 1.X files with different coverage types
and combines them into one BAG 2.X file with georeferenced metadata derived from the 
Survey_Metadata.xml file(s).

....needs work on compositing the various elevation bands together to make sure they 
exactly align geographically at the node level - mabye use gdal to combine/resample?
or make sure interp and mbes surfaces have same origin point and overall shape?????

- right now that work is done to create perfectly aligned grids upstream of this workflow,
meaning all input bags need to be aligned ahead of time.

- also need to figure out what we need to do to update the embedded bag metadata xml information, 
like saying it's a BAG v2.X, etc...' - fixed the version update, but still need to think about 
what else to update in the embedded bag metadata xml
"""

import shutil
import bagPy as BAG
import numpy as np
import h5py
from osgeo import gdal
from lxml import etree as ET
import xml.etree.ElementTree as StdET
import re
import os
from datetime import datetime, timezone
gdal.DontUseExceptions()

namespaces = {
    'gmd': 'http://www.isotc211.org/2005/gmd', 'gco': 'http://www.isotc211.org/2005/gco',
    'gmi': 'http://www.isotc211.org/2005/gmi', 'gml': 'http://www.opengis.net/gml/3.2',
    'bag': 'http://www.opennavsurf.org/schema/bag', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}
StdET.register_namespace("gmd", namespaces['gmd'])
StdET.register_namespace("gco", namespaces['gco'])
StdET.register_namespace("gmi", namespaces['gmi'])
StdET.register_namespace("gml", namespaces['gml'])
StdET.register_namespace("bag", namespaces['bag'])
StdET.register_namespace("xsi", namespaces['xsi'])

#how to actually read the bag metadata xml 
def get_bag_metadata_for_fix(file_path):
    with h5py.File(file_path, 'r') as bag_file:
        xml_metadata = bag_file['BAG_root']['metadata'][()]
        xml_metadata_str = b''.join(xml_metadata).decode('utf-8')
        metadata = StdET.fromstring(xml_metadata_str)
        # print(metadata)
        return metadata
    
#helper func to fix the erroneous cornerPoints in the bag xml metadata from caris-derived bags
def update_corner_points(metadata):
    try:
        row_element = metadata.findall(".//gmd:dimensionSize/gco:Integer", namespaces)[0]
        col_element = metadata.findall(".//gmd:dimensionSize/gco:Integer", namespaces)[1]
        x_res_element = metadata.findall(".//gmd:resolution/gco:Measure", namespaces)[0]
        y_res_element = metadata.findall(".//gmd:resolution/gco:Measure", namespaces)[1]
        coordinates_element = metadata.find(".//gml:coordinates", namespaces)
        if None in (row_element, col_element, x_res_element, y_res_element, coordinates_element):
            raise ValueError("Missing metadata elements for grid parameters.")
        rows, cols = int(row_element.text), int(col_element.text)
        x_res, y_res = float(x_res_element.text), float(y_res_element.text)
        coords = [float(c) for coord_pair in coordinates_element.text.strip().split() for c in coord_pair.split(',')]
        if len(coords) != 4: raise ValueError("Unexpected coordinate format.")
        sw_x, sw_y, ne_x, ne_y = coords
        corrected_ne_x = sw_x + (cols - 1) * x_res
        corrected_ne_y = sw_y + (rows - 1) * y_res
        coordinates_element.text = f"{sw_x},{sw_y} {corrected_ne_x},{corrected_ne_y}"
        return metadata
    except Exception as e:
        print(f"Error updating corner points: {str(e)}"); return None

#helper func to fix the erroneous cornerPoints in the bag xml metadata from caris-derived bags
def fix_bag_corner_points(input_path, output_path):
    try:
        shutil.copyfile(input_path, output_path)
        metadata = get_bag_metadata_for_fix(output_path)
        updated_metadata = update_corner_points(metadata)
        if updated_metadata is None: raise ValueError("Failed to update metadata.")
        modified_metadata_xml = StdET.tostring(updated_metadata, encoding="unicode")
        with h5py.File(output_path, 'r+') as bag_file:
            del bag_file['BAG_root/metadata']
            metadata_array = np.array(list(modified_metadata_xml), dtype='S1')
            bag_file.create_dataset("BAG_root/metadata", data=metadata_array, maxshape=(None,))
        print(f"Corner points fixed for '{os.path.basename(output_path)}'")
        return True
    except Exception as e:
        print(f"Error fixing BAG corner points for '{os.path.basename(input_path)}': {str(e)}"); return False

def parse_survey_metadata(xml_path, target_grid_name):
    if not os.path.exists(xml_path): print(f"Error: XML file not found at {xml_path}"); return None
    try:
        parser = ET.XMLParser(remove_blank_text=True)
        tree = ET.parse(xml_path, parser)
        root = tree.getroot()
        ns = root.nsmap
    except Exception as e: print(f"Error parsing XML file '{os.path.basename(xml_path)}': {e}"); return None
    def get_text(parent, path, default=''):
        element = parent.find(path, ns)
        if element is not None and element.text:
            raw_text = element.text.strip(); sanitized_text = raw_text.replace('\u2013', '-'); return sanitized_text
        return default
    def get_bool(parent, path): return get_text(parent, path).lower() == 'yes'
    def get_float(parent, path, is_percentage=False):
        text_val = get_text(parent, path)
        if not text_val or text_val.lower() == 'n/a': return 0.0
        match = re.search(r'[\d\.]+', text_val)
        if match: num = float(match.group(0)); return num / 100.0 if is_percentage else num
        return 0.0
    metadata_root = root.find('smd:metadata', ns)
    if metadata_root is None: return None
    target_grid_block = None
    for grid_block in metadata_root.findall('smd:grid', ns):
        grid_name = get_text(grid_block, './smd:gridName')
        if grid_name == target_grid_name:
            target_grid_block = grid_block; break
    if target_grid_block is None: print(f"Error: Could not find grid block for '{target_grid_name}' in the XML file."); return None
    
    is_interpolated = get_bool(target_grid_block, './smd:coverageAssessment/smd:interpolated')
    bathy_coverage_value = not is_interpolated

    parsed_data = {
        'source_institution': get_text(metadata_root, './smd:poc/smd:responsibleParty'),
        'source_survey': get_text(metadata_root, './smd:survey/smd:uniqueId'),
        'survey_date_start': get_text(metadata_root, './smd:date/smd:start'),
        'survey_date_end': get_text(metadata_root, './smd:date/smd:end'),
        'licenseName': get_text(metadata_root, './smd:dataLicense/hsd:spdx/hsd:licenseIdentifier', default='Not assigned'),
        'licenseURL': get_text(metadata_root, './smd:dataLicense/hsd:spdx/hsd:licenseDeed', default=''),
        'significant_features_detected': get_bool(target_grid_block, './smd:detection/smd:significantFeature'),
        'feature_least_depth_found': get_bool(target_grid_block, './smd:detection/smd:leastDepth'),
        'coverage': get_bool(target_grid_block, './smd:coverageAssessment/smd:fullSeafloor'),
        'bathy_coverage': bathy_coverage_value,
        'feature_size': get_float(target_grid_block, './smd:detection/smd:size/smd:fixed'),
        'feature_size_var': get_float(target_grid_block, './smd:detection/smd:size/smd:variable', is_percentage=True),
        'horizontal_uncert_fixed': get_float(target_grid_block, './smd:uncertainty/smd:horizontal/smd:fixed'),
        'horizontal_uncert_var': get_float(target_grid_block, './smd:uncertainty/smd:horizontal/smd:variable', is_percentage=True),
    }
    return parsed_data

def add_process_history(bag_path):
    print("Adding processing history to XML")
    try:
        metadata = get_bag_metadata_for_fix(bag_path)
        lineage_element = metadata.find(".//gmd:lineage/gmd:LI_Lineage", namespaces)
        if lineage_element is None:
            print("  Warning: Could not find <gmd:LI_Lineage> element. Skipping process step.")
            return
        now_utc = datetime.now(timezone.utc)
        timestamp = now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        process_step = StdET.SubElement(lineage_element, "gmd:processStep")
        li_process_step = StdET.SubElement(process_step, "gmd:LI_ProcessStep")
        description = StdET.SubElement(li_process_step, "gmd:description")
        char_string = StdET.SubElement(description, "gco:CharacterString")
        char_string.text = "Composite BAG created using custom Python script developed by NOAA Office of Coast Survey. Georeferenced metadata layer added via the bagPy library. Elevation, uncertainty, and keys layers composited from source files."
        datetime_element = StdET.SubElement(li_process_step, "gmd:dateTime")
        datetime_val = StdET.SubElement(datetime_element, "gco:DateTime")
        datetime_val.text = timestamp
        modified_metadata_xml = StdET.tostring(metadata, encoding="unicode")
        with h5py.File(bag_path, 'r+') as bag_file:
            del bag_file['BAG_root/metadata']
            metadata_array = np.array(list(modified_metadata_xml), dtype='S1')
            bag_file.create_dataset("BAG_root/metadata", data=metadata_array, maxshape=(None,))
        print("Successfully added processing step.")
    except Exception as e:
        print(f"Error adding process step: {e}")

def create_bag_v2x():
    """Main function to run the BAG processing workflow."""
    
    # ********IMPORTANT********
    # Define the input files to run the script (bag files and Survey_Metadata.xml files)
    # ***The order of this list determines precedence for metadata record creation.
    # Layers that should overwrite others (e.g. observed data like MBES or SBES) should be LAST in this list.

    OUTPUT_BAG_PATH = r"E:\bag 2.0 project\H12286_MB_1m_MLLW_v2.1.bag"
    
    DATA_LAYERS = [
        #lower precedence layers come first - put the interp bag(s) here
        {
            'key': 1, 'name': "Interpolated",
            'data_path': r"E:\bag 2.0 project\interpytester2\H12286_MB_1m_MLLW_interp_1of2.bag",
            'metadata_path': r"E:\bag 2.0 project\H12286_Survey_Metadata.xml"
        },
        #higher precedence layers come last - always choose the non-interpolated bag(s) here
        {
            'key': 2, 'name': "MBES",
            'data_path': r"E:\bag 2.0 project\H12286_MB_1m_MLLW_1of2.bag",
            'metadata_path': r"E:\bag 2.0 project\H12286_Survey_Metadata.xml"
        },
    ]

    print("starting the BAG conversion and compositing process from multiple v1.x bags to one v2.x bag")

    # PREPROCESSING STEP: Fix corner points
    print("preprocessing to fix the corner points on input BAG files due to caris bug")
    for layer in DATA_LAYERS:
        if layer['data_path'] and layer['data_path'].lower().endswith('.bag') and os.path.exists(layer['data_path']):
            original_path = layer['data_path']
            fixed_path = original_path.replace('.bag', '_fixed.bag')
            if fix_bag_corner_points(original_path, fixed_path):
                layer['data_path'] = fixed_path
            else:
                print(f"FATAL: Could not fix corner points for {os.path.basename(original_path)}. Exiting."); return
    
    active_layers = [lyr for lyr in DATA_LAYERS if lyr['data_path']]
    if not active_layers: print("No active layers found. Exiting."); return

    base_bag_path = active_layers[-1]['data_path']

    # STEP 1: Copy the template file
    print(f"Step 1: Copying template '{os.path.basename(base_bag_path)}'")
    try:
        shutil.copy(base_bag_path, OUTPUT_BAG_PATH)
        # print("base bag copied successfully")
    except Exception as e: print(f"Error copying base file: {e}. Exiting."); return

    # STEP 2: Update BAG Version using h5py, then Populate metadata records
    print("Step 2: Updating BAG version and populating metadata records")
    try:
        with h5py.File(OUTPUT_BAG_PATH, 'a') as f:
            bag_root = f['/BAG_root']
            bag_root.attrs['Bag Version'] = np.string_("2.1.0") #have to keep this updated to whatever verion bagPy you're using
            print("BAG version successfully set to 2.1.0.")
    except Exception as e:
        print(f"An error occurred while setting BAG version: {e}")
        return

    record_indices = {}
    dataset = None
    try:
        dataset = BAG.Dataset.openDataset(OUTPUT_BAG_PATH, BAG.BAG_OPEN_READ_WRITE)
        definition = BAG.METADATA_DEFINITION_NOAA_OCS_2022_10
        georefMetaLayer = dataset.createGeorefMetadataLayer(BAG.DT_UINT16, BAG.NOAA_OCS_2022_10_METADATA_PROFILE, "Elevation", definition, 100, 6)
        valueTable = georefMetaLayer.getValueTable()
        for layer in active_layers:
            original_filename = os.path.basename(layer['data_path'].replace('_fixed',''))
            target_grid_name = os.path.splitext(original_filename)[0]
            metadata = parse_survey_metadata(layer['metadata_path'], target_grid_name)
            if metadata:
                print(f"Defining record for layer: '{layer['name']}'")
                metadata['source_survey'] = target_grid_name
                
                record = BAG.CreateRecord_NOAA_OCS_2022_10(
                    metadata['significant_features_detected'], 
                    metadata['feature_least_depth_found'],
                    metadata['feature_size'], 
                    metadata['feature_size_var'],
                    metadata['coverage'], 
                    metadata['bathy_coverage'],
                    metadata['horizontal_uncert_fixed'], 
                    metadata['horizontal_uncert_var'],
                    metadata['survey_date_start'], 
                    metadata['survey_date_end'],
                    metadata['source_institution'], 
                    metadata['source_survey'],
                    0, #hard-coded the source_survey_index as 0 per g.rice
                    metadata['licenseName'], 
                    metadata['licenseURL']
                )
                
                record_index = valueTable.addRecord(record)
                record_indices[layer['key']] = record_index
                print(f"Record added at index {record_index}, with sourceSurveyIndex=0.")
                # next_record_index += 1
    except Exception as e:
        print(f"An error occurred during metadata population: {e}")
        return
    finally:
        if 'dataset' in locals() and dataset:
            dataset.close()
            print('bagpy dataset is closed and releases the lock')

    
    # STEP 3: Generate composite grids in memory
    print("Step 3: Generating composite grids in memory")
    try:
        template_ds = gdal.Open(base_bag_path, gdal.GA_ReadOnly)
        nodata_value = template_ds.GetRasterBand(1).GetNoDataValue()
        base_rows, base_cols = template_ds.RasterYSize, template_ds.RasterXSize
        composite_elevation = np.full((base_rows, base_cols), nodata_value, dtype=np.float32)
        composite_uncertainty = np.full((base_rows, base_cols), nodata_value, dtype=np.float32)
        keys = np.zeros((base_rows, base_cols), dtype=np.uint16)
        template_ds = None 
        for layer in active_layers:
            real_index = record_indices.get(layer['key'])
            if real_index is not None and os.path.exists(layer['data_path']):
                print(f"Pasting data from layer: '{layer['name']}'")
                layer_ds = gdal.Open(layer['data_path'], gdal.GA_ReadOnly)
                if layer_ds:
                    min_rows, min_cols = min(base_rows, layer_ds.RasterYSize), min(base_cols, layer_ds.RasterXSize)
                    layer_elev = layer_ds.GetRasterBand(1).ReadAsArray(0, 0, min_cols, min_rows)
                    layer_uncert = layer_ds.GetRasterBand(2).ReadAsArray(0, 0, min_cols, min_rows)
                    layer_mask = layer_elev != nodata_value
                    composite_elevation[:min_rows, :min_cols][layer_mask] = layer_elev[layer_mask]
                    composite_uncertainty[:min_rows, :min_cols][layer_mask] = layer_uncert[layer_mask]
                    keys[:min_rows, :min_cols][layer_mask] = real_index
                layer_ds = None
        keys_to_write = np.flipud(keys)
        print("Composite grids generated successfully.")
    except Exception as e: print(f"An error occurred during composite generation: {e}"); return

    # STEP 4: Write final composite grids to BAG file
    print("Step 4: Writing composite grids to BAG file")
    try:
        with h5py.File(OUTPUT_BAG_PATH, 'a') as f:
            f['/BAG_root/elevation'][...] = np.flipud(composite_elevation)
            f['/BAG_root/uncertainty'][...] = np.flipud(composite_uncertainty)
            f.move('/BAG_root/georef_metadata/Elevation', '/BAG_root/georef_metadata/NOAA_OCS_2022_10')
            keys_path = '/BAG_root/georef_metadata/NOAA_OCS_2022_10/keys'
            if keys_path in f: del f[keys_path]
            f.create_dataset(keys_path, data=keys_to_write, chunks=(100, 100), compression=6)
            print("Composite grids and keys written successfully.")
    except Exception as e: print(f"An error occurred during h5py finalization: {e}"); return

    # STEP 5: Finalize XML metadata
    add_process_history(OUTPUT_BAG_PATH)

    print("done." f" output bag v2.x: {OUTPUT_BAG_PATH}")

if __name__ == "__main__":
    create_bag_v2x()
