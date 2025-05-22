# MERA-2 Data in Polaris

MERRA-2 data is hosted by NASA and stored in the NASA GES Data and Information Services Center, and is being migrated to Amazon S3 so the data and tools and services are in the same location (our idea with Polaris). Several visualization tools are available but are static and limited in terms of data and indices for plotting. We will download, pre-aggregate, and store the data in Polaris to evaluate the query processor of Polaris as we did with the ERA5 data. Each step will be timed to give an idea of the time it takes for the offline/pre-processing of data for Polaris.  

## Downloading the data
0. Create virtual environment for project.
        bash init_venv.sh
        source venv/bin/activate
1. Obtain an Earthdata login to request data following [this documentation](https://disc.gsfc.nasa.gov/information/documents?title=Data%20Access).
2. Generate Earthdata prerequisite files `.netrc, .dodsrc` and `.urs_cookies` using code python code in [this documentation](https://disc.gsfc.nasa.gov/information/howto?title=How%20to%20Generate%20Earthdata%20Prerequisite%20Files). Code in `../merra_2/download/generate_prereq_files.py`.
3. Download the following MERRA-2 data (code in `../merra_2/download/download_data.py`): 

        Name: M2I3NXGAS

        Description: 2-dimensional, 3-hourly. Instantaneous, Single-level, Assimilation, Aerosol Optical Depth Analysis Version 5.12.4

        Variable: Aerosol Optical Depth Analysis (AODANA)

        Spatial Resolution: 0.5 x 0.625 degrees

        Temporal Resolution: 3 hours

        Citation: Global Modeling and Assimilation Office (GMAO) (2015), MERRA-2 inst3_2d_gas_Nx: 2d,3-Hourly,Instantaneous,Single-Level,Assimilation,Aerosol Optical Depth Analysis V5.12.4, Greenbelt, MD, USA, Goddard Earth Sciences Data and Information Services Center (GES DISC), Accessed: [Data Access Date], 10.5067/HNGA0EWW0R09

        <!-- Name: M2I1NXINT (inst1_2d_int_Nx)

        Description: 2d, 1-hourly, Instantaneous, Single-level, Assimilation, Vertically Integrated Diagnostics V5.12.4 

        Variable: Total precipitable water

        Spatial Resolution: 0.5 ° x 0.625 °

        Temporal Resolution: 1 hour -->
Code was taken from the [python](https://disc.gsfc.nasa.g`ov/information/howto?keywords=python&title=How%20to%20Access%20GES%20DISC%20Data%20Using%20Python) and [API](https://disc.gsfc.nasa.gov/information/howto?keywords=level%203&title=How%20to%20Use%20the%20Web%20Services%20API%20for%20Subsetting%20MERRA-2%20Data) documentation. Note the data is downloaded one **"granule"** at a time, which is one day. We concatenate them by year to decrease the number of `.nc` files to process. 

**Notes on data download:**
* The server stopped responding for a whole evening. No helpful error or information given.
* Some years downloaded completely without any issue. For most of the years, not all granules (days) of the year downloaded, so some days and range of days had to be re-requested so they could be downloaded before the data was concatenated into a year file. This request time was similar to the original request time even when only a couple URLs (~2-10 instead of ~365) were requested, and was not added to the original request time.
* The download time does not count the concatenation time needed to combine the granules into years and then all the yearly files into one file with the whole time range. 
* Concatenating all the years into one took about 1 minute.
* The size of `AODANA_2015-2024.nc` is 692MB.

## Aggregating the data
We aggregate the data from a **3-hour** interval temporal resolution to *daily, monthly,* and *yearly* resolutions, and we spatially aggregate the data at each temporal resolution from a **0.5 x 0.625** coordinate degree spatial resolution to *1 x 1.25* degree resolution and *1.5 x 1.875* degree resolution.

The code can be found in `../merra_2/aggregation/agg_driver.py`)

## Measuring performance
**Timing data request and downloading:** URLs for each year were requested simultaneously, so we have one request time for each year. Then for each day we kept track of the download time. Thus, we can get the following statistics:
* Total request and downloading time
* Total request time
* Total download time
* Average year request time 
* Average year download time
* Average day download time

Result for data:
        Number of years: 10
        Number of days: 3653

                REQUEST TIME:
                        Total (minutes): 6.16
                        Average seconds per year: 36.97

                DOWNLOAD TIME:
                        Total (minutes): 485.34
                        Average seconds per day: 7.97
                        Average minutes per year: 48.53

                TOTAL DATA AQUISITION TIME:
                        seconds: 29490.32
                        minutes: 491.51
                        hours: 8.19

The code for this is in `../merra_2/download/performance.py`

## Running experiments of Polaris using MERRA-2 data
1. Generate the experiments we want to perform (code in `../NEED TO CODE.csv`)
        * These experiments will be as close as possible to the queries in `iharp3/experiment-kit/round2/tests`, but edited to account for the different dataset.
2. Get the `metadata.csv` file that stores information for Polaris to query the data(code in `get_metadata.py`)
3. Use the drivers in `iharp3/experiment-kit/round2` to perform the tests.
4. Add the results of the experiments to the corresponding *plot_name_all.csv* in `iharp3/experiment-kit/round2/results`.
5. Generate the plots of *Vanilla, TileDB,* and *Polaris* with the additional line for MERRA-2 and Polaris. Code for the plots is in `experiment-kit/round2/figs/code`.

### Experiments
* Figure 6: Impact of Result Size
        * Queries: `round2/tests/5a.csv`
        * Vanilla/TDB/PolarisERA5 results: *5a_all.csv*
        * Plot code: `experiment-kit/round2/figs/code/5aplot.py`

* Figure 5: Impact of Changing Spatial and Temporal Resolutions
        * Queries: `round2/tests/5c.csv`
        * Vanilla/TDB/PolarisERA5 results: *5c_all.csv*
        * Plot code: `experiment-kit/round2/figs/code/5cplot.py`

* Figure 8: Find Time Query Performance
        * Queries: 
        * Vanilla/TDB/PolarisERA5 results: *greenland.csv*
        * Plot code: `experiment-kit/round2/figs/code/fvplot.py`

* Figure 7: Heatmap Query Performance
        * Queries: 
        * Vanilla/TDB/PolarisERA5 results: *heatmap.csv*
        * Plot code: `experiment-kit/round2/figs/code/hmplot.py`
