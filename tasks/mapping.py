import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import cm
import requests
import time
import os
import json
from logger import Logger as logger
from collections import defaultdict
from tqdm import tqdm

# Function to get coordinates using OpenStreetMap Nominatim API with retry logic and rate limiting
def get_osm_coordinates(city, country="Germany", cache=None, retries=5, backoff_factor=1):
    if cache and city in cache:
        logger.info(f"Using cached coordinates for {city}")
        return cache[city] 
    
    url = f"https://nominatim.openstreetmap.org/search?q={city},{country}&format=json&limit=1"
    
    for attempt in range(retries):
        logger.info(f"Attempting to get coordinates for {city} (Attempt {attempt + 1}/{retries})")
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        
        if response.status_code == 200 and response.json():
            data = response.json()[0]
            lat, lon = float(data['lat']), float(data['lon'])
            logger.info(f"Successfully retrieved coordinates for {city}: (lat: {lat}, lon: {lon})")
            return lat, lon
        elif response.status_code == 429:
            logger.warning(f"Rate limit hit for {city}, waiting {backoff_factor} seconds before retrying...")
            time.sleep(backoff_factor)
            backoff_factor *= 2 
        else:
            logger.error(f"Error {response.status_code} while retrieving coordinates for {city}. Retrying...")
            time.sleep(backoff_factor)
            backoff_factor *= 2

    logger.error(f"Failed to get coordinates for {city} after {retries} attempts.")
    return None, None

# Function to load or create cache in the 'reporting' folder
def load_cache(cache_file):
    reporting_folder = os.path.join(os.getcwd(), 'reporting')
    cache_file_path = os.path.join(reporting_folder, cache_file)

    if not os.path.exists(reporting_folder):
        os.makedirs(reporting_folder)

    if os.path.exists(cache_file_path):
        logger.info(f"Loading cache from {cache_file_path}")
        with open(cache_file_path, 'r') as f:
            return json.load(f)
    logger.info(f"No cache found. Starting fresh.")
    return {}

# Function to save cache to a file in the 'reporting' folder
def save_cache(cache, cache_file):
    reporting_folder = os.path.join(os.getcwd(), 'reporting')
    cache_file_path = os.path.join(reporting_folder, cache_file)

    if not os.path.exists(reporting_folder):
        os.makedirs(reporting_folder)

    logger.info(f"Saving cache to {cache_file_path}")
    with open(cache_file_path, 'w') as f:
        json.dump(cache, f)

# Function to generate and save the map
def generate_germany_map(categorized_csv, output_image, cache_file='city_coords_cache.json'):
    logger.info(f"Starting to generate map from {categorized_csv}")
    
    df = pd.read_csv(categorized_csv)
    logger.info(f"Loaded CSV file with {len(df)} rows")

    df_germany = df[df['Country'] == 'Germany']
    logger.info(f"Filtered to {len(df_germany)} rows for Germany")

    city_coords_cache = load_cache(cache_file)
    city_coords = defaultdict(list)

    # Only apply delay when calling the API (for uncached cities)
    uncached_cities = []

    logger.info(f"Fetching coordinates for {len(df_germany['City'].unique())} unique cities...")
    
    for city in tqdm(df_germany['City'].unique(), desc="Processing cities", ncols=100):
        if city in city_coords_cache:
            # Use cached coordinates
            city_coords[city] = city_coords_cache[city]
        else:
            uncached_cities.append(city)
            lat, lon = get_osm_coordinates(city, country="Germany", cache=city_coords_cache)
            if lat and lon:
                city_coords[city] = (lat, lon)
                city_coords_cache[city] = (lat, lon)
                # Only sleep when the API is hit, not for cached data
                time.sleep(1)  
            else:
                logger.warning(f"Skipping city {city} due to missing coordinates.")

    save_cache(city_coords_cache, cache_file)

    city_counts = df_germany.groupby(['City', 'RE_Strategy_Names']).size().unstack(fill_value=0)

    # Load the Germany shapefile from the provided path
    logger.info("Loading Germany shapefile")
    germany_shapefile_path = "helpers/natural_earth/ne_110m_admin_0_countries.shp"
    germany = gpd.read_file(germany_shapefile_path)
    
    # Filter for Germany
    germany = germany[germany['SOVEREIGNT'] == 'Germany']

    logger.info("Plotting the map")
    fig, ax = plt.subplots(figsize=(12, 12))
    germany.plot(ax=ax, color='lightgrey')

    strategies = city_counts.columns
    colors = cm.get_cmap('coolwarm', len(strategies))

    for i, strategy in enumerate(strategies):
        for city, row in city_counts.iterrows():
            if row[strategy] > 0:
                if city in city_coords:
                    lat, lon = city_coords[city]
                    ax.scatter(lon, lat, s=row[strategy] * 50, color=colors(i), alpha=0.6, edgecolor='black', label=strategy)

    # Create a legend with multiple columns and smaller marker size
    legend_elements = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=colors(i), markersize=8, label=strategy) for i, strategy in enumerate(strategies)]
    
    # Set number of columns in the legend
    plt.legend(handles=legend_elements, title="RE Strategies", loc='upper left', bbox_to_anchor=(1, 1), ncol=2, fontsize='small', title_fontsize='medium')

    plt.title('Distribution of Companies in Germany by Circular Economy RE Strategies')

    logger.info(f"Saving map to {output_image}")
    plt.savefig(output_image, dpi=300, bbox_inches='tight')  # Ensure everything fits within the output image

    plt.show()
    logger.info("Map generation completed")