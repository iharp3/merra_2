'''
Download data using GES DISC API
Edited code from NASA's GES DISC documentation:
    https://github.com/nasa/gesdisc-tutorials/blob/main/notebooks/How_to_Use_the_Web_Services_API_for_Subsetting_MERRA-2_Data.ipynb
    Accessed May 7, 2025
     
Code Overview: 
      Open a GES DISC subset service endpoint
      For each year:
          1. Get urls for each granule (day) of the data
          2. Download data from each url
          3. Keep track of download and request time
          4. Combine all daily data into one .nc file IF all granules are downloaded

            IF NOT all granules are downloaded for a year:
                4a. Get urls for the missing granues
                4b. Download data from each url in same directory as downloaded data
                4c. Keep track of download and request time
                4d. Combine all daily data into one .nc file
                
          5. Delete daily .nc files
'''

import sys
import json
import urllib3
import certifi
import requests
from time import sleep
import time
from http.cookiejar import CookieJar
import urllib.request
from urllib.parse import urlencode
import getpass
import os
import xarray as xr

# Define the parameters for all the data subsets
PRODUCT = 'M2I3NXGAS_V5.12.4' 
VARNAMES =['AODANA']
MINLON = -70
MAXLON = -10
MINLAT = 60
MAXLAT = 90

# Create a urllib PoolManager instance to make requests.
HTTP = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
# Set the URL for the GES DISC subset service endpoint
URL = 'https://disc.gsfc.nasa.gov/service/subset/jsonwsp'

# Check if the available disk space at `path` is above `min_free_gb`
def check_disk_space(path='.', min_free_gb=10):
    statvfs = os.statvfs(path)
    free_bytes = statvfs.f_bavail * statvfs.f_frsize
    free_gb = free_bytes / (1024 ** 3)
    # print(f"\t\t\tAvailable space: {free_gb:.2f} GB")
    if free_gb < min_free_gb:
        print(f"\t\tError: Less than {min_free_gb} GB of free space left.")
        sys.exit(1)
    
    return free_gb

# This method POSTs formatted JSON WSP requests to the GES DISC endpoint URL
def get_http_data(request):
    hdrs = {'Content-Type': 'application/json',
            'Accept'      : 'application/json'}
    data = json.dumps(request)       
    r = HTTP.request('POST', URL, body=data, headers=hdrs)
    response = json.loads(r.data)   
    # Check for errors
    if response['type'] == 'jsonwsp/fault' :
        print('\tAPI Error: faulty %s request' % response['methodname'])
        sys.exit(1)
    return response

# Construct JSON WSP request for API method: subset
def get_subset_request(begTime, endTime):
    subset_request = {
        'methodname': 'subset',
        'type': 'jsonwsp/request',
        'version': '1.0',
        'args': {
            'role'  : 'subset',
            'start' : begTime,
            'end'   : endTime,
            'box'   : [MINLON, MINLAT, MAXLON, MAXLAT],
            'crop'  : True, 
            'data': [{'datasetId': PRODUCT,
                  'variable' : VARNAMES[0]
                 }]
        }
    }

    return subset_request

# Submit the subset request to the GES DISC Server
def submit_request(subset_request):
    response = get_http_data(subset_request)
    # Report the JobID and initial status
    myJobId = response['result']['jobId']
    print(f'\nJob ID: '+myJobId)
    print(f'\tJob status: '+response['result']['Status'])
    
    return myJobId

# Construct JSON WSP request for API method: GetStatus
def get_request_status(myJobId):
    status_request = {
        'methodname': 'GetStatus',
        'version': '1.0',
        'type': 'jsonwsp/request',
        'args': {'jobId': myJobId}
    }
    response =  get_http_data(status_request)
    # print(f'\tresponse: {response['result']}\n\t\tstatus: {response['result']['Status']}\n\t\tmessage: {response['result']['Status']}')
    # Check on the job status after a brief nap
    while response['result']['Status'] in ['Accepted', 'Running']:
        sleep(5)
        response = get_http_data(status_request)
        # status  = response['result']['Status']
        # percent = response['result']['PercentCompleted']
        # print (f'\tJob status: %s (%d%c complete)' % (status,percent,'%'))
    if response['result']['Status'] == 'Succeeded' :
        print (f'\tJob Finished:  %s' % response['result']['message'])
        print(f'\t~')
    else : 
        print(f'\tJob Failed: %s' % response['result']['Status'])
        sys.exit(1)

# Construct JSON WSP request for API method: GetResult
def get_urls(myJobId):
    batchsize = 20
    results_request = {
        'methodname': 'GetResult',
        'version': '1.0',
        'type': 'jsonwsp/request',
        'args': {
            'jobId': myJobId,
            'count': batchsize,
            'startIndex': 0
        }
    }

    results = []
    count = 0 
    response = get_http_data(results_request) 
    count = count + response['result']['itemsPerPage']
    results.extend(response['result']['items']) 

    # Increment the startIndex and keep asking for more results until we have them all
    total = response['result']['totalResults']
    while count < total :
        results_request['args']['startIndex'] += batchsize 
        response = get_http_data(results_request) 
        count = count + response['result']['itemsPerPage']
        results.extend(response['result']['items'])
        
    # Check on the bookkeeping
    if (len(results) == total) and (total != 0):
        print(f'\tRetrieved %d out of %d expected urls' % (len(results), total))
    else:
        print(f'\tOnly returned {len(results)} out of {total} expected urls')
        sys.exit(1)

    return results

# Use the requests library to submit the HTTP_Services URLs and write out the results.
def download_data(urls, yr):
    c_bool = True
    for item in urls :

        start_download_t = time.time()

        URL = item['link'] 
        result = requests.get(URL)
        try:
            # DOWNLOAD DATA
            result.raise_for_status()
            outfn = item['label']
            f = open(outfn,'wb')
            f.write(result.content)
            f.close()
            # print(f"\t\t{outfn} is downloaded")
            download_t = time.time() - start_download_t
            # total_t = request_t + download_t # 2015, 2016, 2017

            # SAVE RUNTIMES
            runtime_all = {"label": str(outfn),
                           "download_t": download_t,
                        #    "total_t": total_t,
                       }
            
            with open(f'runtime_info_{yr}.jsonl', 'a') as f:
                f.write(json.dumps(runtime_all) + '\n')
                f.flush()
                os.fsync(f.fileno())

            # CHECK STORAGE SPACE
            check_disk_space()

        except:
            print(f'\t\tError downloading {item['label']}') 
            c_bool = False

    if c_bool:
        print(f"\n\tDownloaded all {yr} files.")
    else:
        print(f'\tDid NOT download all {yr} files')

    return c_bool 

def concat_granules_to_years(yr):
    try:
        # Concatonate all daily nc files to yearly nc files
        homeDir = "/home/uribe055/merra_2/data_d"
        fileNames = [os.path.join(homeDir, f) for f in os.listdir(homeDir) if f.endswith(".nc4")]
        datasets = [xr.open_dataset(f) for f in sorted(fileNames)]
        combined = xr.concat(datasets, dim="time")
        outName = os.path.join("/home/uribe055/merra_2/data_yr", f"{yr}.nc")
        combined.to_netcdf(outName)
        print(f"\tAll {yr} files concatenated.")
    except:
        print(f"\tSomething went wrong with concatenating {yr} files")
        sys.exit(1)

    # Delete daily files
    for f in fileNames:
        os.remove(f)
    print(f"\tDaily {yr} files deleted.")
    print("~")

def concat_years_to_timerange():
    try:
        start_t = time.time()
        homeDir = "/home/uribe055/merra_2/data_yr"
        fileNames = [os.path.join(homeDir, f) for f in os.listdir(homeDir) if f.endswith(".nc")]
        datasets = [xr.open_dataset(f) for f in sorted(fileNames)]
        combined = xr.concat(datasets, dim="time")
        outName = os.path.join("/home/uribe055/merra_2/data", "AODANA_2015-2024.nc")
        combined.to_netcdf(outName)
        end_t = time.time() - start_t
        print(f"\tAll yearly files concatenated.\n\tTime (sec): {round(end_t, 2)}\n\t\t(min): {round(end_t/60, 2)} ")
    except:
        print(f"\tSomething went wrong with concatenating yearly files")
        sys.exit(1)
################################           ################################
################################    MAIN   ################################
################################           ################################

if __name__ == "__main__":

    # Downloading leftover days constants
    leftover_days = False

    # Concatenating days to year constants
    can_concat = False
    if can_concat:
        y = '2018'

    # Run whole years 
    begTime_list = []
    endTime_list = []
    yrs_list = []

    ##### DOWNLOADING LEFTOVER DAYS #####
    if leftover_days:
        for s, e, y in zip(begTime_list, endTime_list, yrs_list):
        
            start_request_run_t = time.time()

            subset_request = get_subset_request(begTime=s, endTime=e)
            jobId = submit_request(subset_request)
            get_request_status(myJobId=jobId)

            results = get_urls(myJobId=jobId)

            # Sort the results into documents and URLs
            docs = []
            urls = []
            for item in results:
                try:
                    if item['start'] and item['end'] : urls.append(item) 
                except:
                    docs.append(item)

            request_run_t = [y, time.time() - start_request_run_t]
            with open(f'request_t.jsonl', 'a') as f:
                f.write(json.dumps(request_run_t) + '\n')
                f.flush()
                os.fsync(f.fileno())
            
            # Download data
            concat_bool = download_data(urls=urls, yr=y)    # returns True if downloaded all urls, False else
            if concat_bool:
                print(f"Downloaded all files: {concat_bool}")
            else:
                print(f'\n###### Missing files. Download them before concatenating year. #####')
                sys.exit(1)
        
    ##### CONCATENATING ALL DAYS IN THE YEAR #####
    elif can_concat: 
        concat_granules_to_years(yr=y)

    ##### RUN MULTIPLE YEARS UNTIL ONE YEAR DOESNT GET COMPLETELY DOWNLOADED #####
    else:
        for s, e, y in zip(begTime_list, endTime_list, yrs_list):
            concat_bool = False # assume you did NOT download all data for this year
            start_request_run_t = time.time()

            subset_request = get_subset_request(begTime=s, endTime=e)
            jobId = submit_request(subset_request)
            get_request_status(myJobId=jobId)

            results = get_urls(myJobId=jobId)

            # Sort the results into documents and URLs
            docs = []
            urls = []
            for item in results:
                try:
                    if item['start'] and item['end'] : urls.append(item) 
                except:
                    docs.append(item)

            # Save urls and request time in case job is canceled
            with open(f'url_list_{y}.json', 'w') as f:
                json.dump(urls, f)
            request_run_t = [y, time.time() - start_request_run_t]
            with open(f'request_t.jsonl', 'a') as f:
                f.write(json.dumps(request_run_t) + '\n')
                f.flush()
                os.fsync(f.fileno())
            
            # Download data
            concat_bool = download_data(urls=urls, yr=y)    # returns True if downloaded all urls, False else

            # Concat daily files into a year file
            if concat_bool:
                concat_granules_to_years(yr=y)
            else:
                print(f'\n###### Missing files. Download them before concatenating year. #####')
                sys.exit(1)
        
        concat_years_to_timerange()