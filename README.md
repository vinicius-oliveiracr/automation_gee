Automated Precipitation Data Pipeline using Google Earth Engine's API

![Python](https://img.shields.io/badge/Python-3.x-blue) ![GEE](https://img.shields.io/badge/Google%20Earth%20Engine-API-green) ![ETL](https://img.shields.io/badge/Data%20Pipeline-ETL-orange) ![Geospatial](https://img.shields.io/badge/Geospatial-Data-lightgrey) ![Hydrology](https://img.shields.io/badge/Hydrological-Modeling-blueviolet)


This project focuses on the automation of a data pipeline for extracting, processing and preparing precipitation data using Google Earth Engine (GEE) API. It ingests geospatial basin data, processes time-series precipitation data, and generates structured outputs to be consumed by the HEC-HMS hydrological model.

The ultimate goal is to reduce manual effort, improve reproducibility and ensures a reliable data preparation workflow for hydrological modelling.

## Overview

The pipeline automates the following steps:

1. Ingestion of geospatial data, using basin's shapefile
2. Authentication and data extraction from Google Earth Engine
3. Download and processing of precipitation time-series data
4. Transformation and preparation of data from CSV to DSS, required by the hydrological model

To improve performance and avoid Google Earth Engine API limitations, the time range is split into 30-day intervals during data extraction.

## Tecnologies and Tools Used

- Python
- Google Earth Engine (Python API)
- Pandas
- Geopandas
- OAuth2 / Service Account authentication
- Geospatial data processing tools
- CSV data handling
- Git for Version Control

## Project Structure

- auth.py → Handles authentication using Google OAuth2 and service accounts
- config.py → Centralizes environment variables, configuration parameters, and worflow setup
- data.py → Implements data extraction and processing pipeline using GEE
- dss_generator.py → Transforms precipitation time-series data from CSV into DSS format for HEC-HMS. Handles temporal filtering, subbasin-based grouping, time-series construction, and automated generation of gage metadata for model integration.
- geoprocessor.py → Performs geospatial data preprocessing and feature engineering. Converts and validates basin geometries, generates raster masks for subbasins, and computes zonal statistics from auxiliary rasters, producing structured geospatial outputs consumed by downstream data extraction and modeling steps.
- hms_file_generator.py → Automates the generation of HEC-HMS input configuration files using templated workflows. Transforms structured pipeline outputs into gage, meteorological, and control files required for hydrological simulations.
- main.py → Pipeline orchestrator that manages end-to-end execution of geospatial ETL workflows, from data ingestion and transformation to generation of structured outputs for hydrological modeling systems.

## Key Features

- End-to-end automated geospatial ETL pipeline
- Scalable data extraction from Google Earth Engine
- Time-series processing and validation
- Automated generation of DSS and HEC-HMS input files
- Modular and reproducible architecture
