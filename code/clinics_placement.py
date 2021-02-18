# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import rasterio
import rasterio.mask
import rasterstats
import seaborn as sns

from rasterio.plot import show
from rasterstats import zonal_stats
from shapely.geometry import Point

# -

# Read in shapes of Malawi TAs
gdf_country = gpd.read_file('../data/MWI_adm2.shp')

# Read in map of areas currently covered by health clinics
gdf_covered = gpd.read_file('../data/SA_Best Case/SA_BestCase.shp').to_crs('epsg:4326')

# Subtract the covered areas from the country to get the uncovered area
gdf_uncovered = gpd.overlay(gdf_country, gdf_covered, how='difference')

# Convert the uncovered area into a single giant polygon
uncovered_polygon = gdf_uncovered.unary_union

# Open the raster file of 1km² population grid
with rasterio.open('../data/mwi_ppp_2020_1km_Aggregated_UNadj.tif') as tif_unpop:
    band = tif_unpop.read(1)
    shape = tif_unpop.shape
    print(f"Processing {shape[0]} × {shape[1]} raster file")
    
    # Apply the uncovered shape as a mask, keeping only population in the uncovered area
    uncovered_raster, uncovered_transform = rasterio.mask.mask(tif_unpop, uncovered_polygon, crop=True)
    uncovered_band = uncovered_raster[0]
    
    # Replace negative numbers with 0 for proper calculation
    uncovered_band[uncovered_band < 0] = 0


# Plot a raster map replacing 0 with negative numbers for high contrast
def show_band(band):
    band_copy = band.copy()
    band_copy[band_copy == 0] = -99999
    show(band_copy)


# Get a list of points in an 11x11 circle around idx (row, col)
def points_in_circle(band, idx):
    points = []
    row, col = idx
    
    for y in range(max(0, row - 5), min(band.shape[0], row + 6)):
        xwidth = min(11, 15 - abs(row - y) * 2)
        xrange = range(max(0, col - (xwidth - 1) // 2), min(band.shape[1], col + (xwidth) // 2 + 1))
        points += [(y,x) for x in xrange]
        
    return points


# Sum the population that would be covered in the 11x11 circle around idx (row, col)
def point_coverage(band, idx):
    population = 0
    
    for point in points_in_circle(band, idx):
        population += band[point]
    
    return population


# Set all populations in the newly covered area around idx (row, col) to 0
def remove_point(band, idx):
    out_band = band.copy()
    
    for point in points_in_circle(band, idx):
        out_band[point] = 0
    
    return out_band


# Calculate the population in each point's 11x11 circle coverage area
def map_coverage(uncovered_band):
    coverage = uncovered_band.copy()
    for idx, value in np.ndenumerate(uncovered_band):
        coverage[idx] = point_coverage(uncovered_band, idx)
    return coverage


# After removing the point, recalculate all coverage in a 23x23 square around the removed point
def update_coverage(uncovered_band, coverage, idx):
    out_coverage = coverage.copy()
    row, col = idx
    
    for y in range(max(0, row - 11), min(uncovered_band.shape[0], row + 12)):
        for x in range(max(0, col - 11), min(uncovered_band.shape[1], col + 12)):
            out_coverage[y,x] = point_coverage(uncovered_band, (y,x))
            
    return out_coverage


# +
# Place n clinics in order of population served

num_clinics = 1000
uncovered = uncovered_band.copy()
coverage = map_coverage(uncovered)

geometries = []
newly_covered = []

for n in range(0, num_clinics):
    # Get the potential clinic location index with the highest population covered
    top_clinic_idx = np.unravel_index(coverage.argmax(), coverage.shape)
    
    # Calculate the latitude and longitude of that location
    lon, lat = tif_unpop.xy(top_clinic_idx[0], top_clinic_idx[1])
    
    print(f'Clinic #{n + 1} placed at ({lat:.2f}°, {lon:.2f}°) covering {coverage[top_clinic_idx]:.0f} people')
    
    # Save the coordinates and newly covered population for output
    geometries.append(Point(lon, lat))
    newly_covered.append(coverage[top_clinic_idx])    
    
    # Remove the new location from the uncovered map and update the coverage array
    uncovered = remove_point(uncovered, top_clinic_idx)
    coverage = update_coverage(uncovered, coverage, top_clinic_idx)

# Add all the points and newly served populations to a geodataframe
gdf_new_clinics = gpd.GeoDataFrame({'geometry': geometries, 'newly_covered': newly_covered}, crs='epsg:4326')

# -

# Add TA/district information to the new clinics using a spatial join
gdf_clinics = gpd.sjoin(gdf_new_clinics.reset_index(), gdf_country).sort_values('newly_covered', ascending=False)
gdf_clinics['priority'] = gdf_clinics['index'] + 1
gdf_clinics.drop(columns=['index_right','index'], inplace=True)

gdf_clinics

# Save the new clinics to a shapefile
gdf_clinics.to_file('../out/health_clinics.shp')
