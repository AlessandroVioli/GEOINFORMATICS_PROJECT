# GEOINFORMATICS_PROJECT
Automated Downloader for Earth Observation Data: From Complex Search to Simple CSV

## Requirements

Here are the Python libraries required to run this project. It’s recommended to create a virtual environment before installation.

# Basic libraries
numpy
pandas
geopandas
shapely
fiona
pyproj
rasterio
rioxarray
xarray

# WebGIS / mapping / frontend-backend
flask
flask_restful
folium
leaflet     # or see alternative
ipyleaflet   # if using Jupyter‐based map interactivity

# Database / PostGIS
psycopg2    # PostgreSQL adapter
sqlalchemy
geoalchemy2

# Geospatial APIs / downloads
requests
tqdm
beautifulsoup4   # if scraping webpages
cartopy           # optional for advanced mapping

# Data download / processing
gdal
osgeo
snappy            # if using compressed formats
pycurl            # optional

# Authentication / OAuth (for e.g. Google Earth Engine)
oauth2client
google-oauth-library   # or google_auth

# Logging / utilities
click
loguru
pytest             # for testing
pytest-cov         # for coverage

# Others you actually use in your project
… (add here)
