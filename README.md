The project is organized considering the necessity of automating the extraction of precipitation data using Google Earth Engine,
manipulating CSV archives received and feeding it to a .dss file. 

- main.py → control workflows, given the necessary information
  - GEE workflow:
    - creating and checking if the needed files are on .env for each class
    - authentication with Google OAuth2
    - Extracting geographic data from shapefile previously provided by the user, in order to extract precipitation data. Process .shp file into a GeoJSON for better performance
    - Download data from GEE, creating a series of .csv files. Time period is split in a 30 day period for better performance at GEE API
  - DSS workflow:

- auth.py → authentication for using Google Earth Engine through a service account previously created with the necessary credentials.
- config.py → create, manipulate and use the required files to automate the data extraction from GEE and creation of DSS file
- data.py → data extraction workflow
    - PrecipitationDownloader (class) → calls Config class and creates a folder destined to store the .csv files extracted from GEE
    - download_data → method where it splits the time frame into a period of 30 days, focusing on better performance with GEE API
    - _create_feature_collection → converts GeoDataFrame into ee.FeatureCollection, in order to use previously obtained data through geoprocessing onto extracting precipitation data
    - _get_image_collection → 
  - tranforming shapefile into a GeoDataframe using user especific SRC, then converting it to GeoJSON, in order to manipulate it;
  - using .tif files to create zones of interest
  - 
- 
