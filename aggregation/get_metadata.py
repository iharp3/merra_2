'''
Create the metadata.csv file Polaris uses to query the data
    Columns: ['variable', 'max_lat', 'min_lat', 'max_lon', 'min_lon', 'start_datetime', 'end_datetime', 'temporal_resolution', 'spatial_resolution', 'aggregation', 'file_path']

'''

import time
import os
import pandas as pd
import xarray as xr
import re

def extract_metadata_from_filename(filename):
    """Extract variable, spatial_resolution, temporal_resolution, aggregation from filename."""
    pattern = r'\d{4}_(.*?)_(.*?)_(.*?)\.nc$'
    match = re.match(pattern, filename)
    if match:
        # variable, spatial_resolution, temporal_resolution, aggregation = match.groups()   # for concatenated file
        spatial_resolution, temporal_resolution, aggregation = match.groups()   # for yearly files
        return "AODANA", spatial_resolution, temporal_resolution, aggregation
    else:
        return "AODANA", 0, "hour", "none"
        # return None, None, None, None 

def get_nc_metadata(nc_path):
    """Extract metadata from a NetCDF file."""
    try:
        ds = xr.open_dataset(nc_path)

        min_lat = float(ds['lat'].min())
        max_lat = float(ds['lat'].max())
        min_lon = float(ds['lon'].min())
        max_lon = float(ds['lon'].max())

        time_vals = pd.to_datetime(ds['time'].values)
        start_datetime = time_vals.min()
        end_datetime = time_vals.max()

        filename = os.path.basename(nc_path)
        variable, spatial_resolution, temporal_resolution, aggregation = extract_metadata_from_filename(filename)

        return {
            'variable': variable,
            'max_lat': max_lat,
            'min_lat': min_lat,
            'max_lon': max_lon,
            'min_lon': min_lon,
            'start_datetime': start_datetime,
            'end_datetime': end_datetime,
            'temporal_resolution': temporal_resolution,
            'spatial_resolution': spatial_resolution,
            'aggregation': aggregation,
            'file_path': nc_path
        }
    except Exception as e:
        print(f"Error processing {nc_path}: {e}")
        return None

def process_folder(folder_path, output_csv='TIMED_metadata.csv'):
    """Process all .nc files in a folder and write metadata to a CSV."""
    metadata_list = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.nc'):
            full_path = os.path.join(folder_path, filename)
            metadata = get_nc_metadata(full_path)
            if metadata:
                metadata_list.append(metadata)

    df = pd.DataFrame(metadata_list)
    df.to_csv(output_csv, index=False)
    print(f"Metadata saved to {output_csv}")

if __name__ == "__main__":
    folder_path = "/home/uribe055/merra_2/data_yr"

    start_time = time.time()
    process_folder(folder_path)
    end_time = time.time() - start_time


    print(f"Metadata:\n\tseconds: {end_time}\n\tminutes: {end_time/60}\n\thours: {end_time/(60*60)}")