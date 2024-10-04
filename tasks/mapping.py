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
        
        # If request is successful (status code 200), extract coordinates
        if response.status_code == 200 and response.json():
            data = response.json()[0]
            lat, lon = float(data['lat']), float(data['lon'])
            logger.info(f"Successfully retrieved coordinates for {city}: (lat: {lat}, lon: {lon})")
            return lat, lon
        
        # If the API returns a rate-limit error (HTTP 429), back off and retry
        elif response.status_code == 429:
            logger.warning(f"Rate limit hit for {city}, waiting {backoff_factor} seconds before retrying...")
            time.sleep(backoff_factor)
            backoff_factor *= 2 
        
        # For other errors, log and continue to retry
        else:
            logger.error(f"Error {response.status_code} while retrieving coordinates for {city}. Retrying...")
            time.sleep(backoff_factor)
            backoff_factor *= 2  # Exponential backoff

    # If all retries fail, return None
    logger.error(f"Failed to get coordinates for {city} after {retries} attempts.")
    return None, None

# Function to load or create cache
def load_cache(cache_file):
    if os.path.exists(cache_file):
        logger.info(f"Loading cache from {cache_file}")
        with open(cache_file, 'r') as f:
            return json.load(f)
    logger.info(f"No cache found. Starting fresh.")
    return {}

# Function to save cache to a file
def save_cache(cache, cache_file):
    logger.info(f"Saving cache to {cache_file}")
    with open(cache_file, 'w') as f:
        json.dump(cache, f)

# Function to generate and save the map
def generate_germany_map(categorized_csv, output_image, cache_file='city_coords_cache.json'):
    logger.info(f"Starting to generate map from {categorized_csv}")
    
    # Load the CSV file containing categorized companies
    df = pd.read_csv(categorized_csv)
    logger.info(f"Loaded CSV file with {len(df)} rows")

    # Filter only German cities
    df_germany = df[df['Country'] == 'Germany']
    logger.info(f"Filtered to {len(df_germany)} rows for Germany")

    # Load existing city coordinates cache (if any)
    city_coords_cache = load_cache(cache_file)

    # Create a dictionary to store city coordinates
    city_coords = defaultdict(list)

    # Get coordinates for each city (cached in the dictionary)
    logger.info(f"Fetching coordinates for {len(df_germany['City'].unique())} unique cities...")
    
    # Add a progress bar
    for city in tqdm(df_germany['City'].unique(), desc="Processing cities", ncols=100):
        lat, lon = get_osm_coordinates(city, country="Germany", cache=city_coords_cache)
        if lat and lon:
            city_coords[city] = (lat, lon)
            city_coords_cache[city] = (lat, lon)  # Update cache with the new coordinates
        else:
            logger.warning(f"Skipping city {city} due to missing coordinates.")
        time.sleep(1)  # Add a basic delay between each request to reduce API load

    # Save the updated cache
    save_cache(city_coords_cache, cache_file)

    # Count the number of companies in each city based on RE strategies
    city_counts = df_germany.groupby(['City', 'Categorization']).size().unstack(fill_value=0)

    # Load the shapefile of Germany
    logger.info("Loading Germany shapefile")
    germany = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    germany = germany[germany['name'] == 'Germany']

    # Plot Germany map
    logger.info("Plotting the map")
    fig, ax = plt.subplots(figsize=(12, 12))
    germany.plot(ax=ax, color='lightgrey')

    # Generate color map for the cities based on the number of companies in each RE strategy
    strategies = city_counts.columns
    colors = cm.get_cmap('coolwarm', len(strategies))

    # Plot each city based on its company count
    for i, strategy in enumerate(strategies):
        for city, row in city_counts.iterrows():
            if row[strategy] > 0:
                if city in city_coords:
                    lat, lon = city_coords[city]
                    ax.scatter(lon, lat, s=row[strategy] * 50, color=colors(i), alpha=0.6, edgecolor='black', label=strategy)

    # Create a legend
    legend_elements = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=colors(i), markersize=10, label=strategy) for i, strategy in enumerate(strategies)]
    plt.legend(handles=legend_elements, title="RE Strategies", loc='upper right')

    # Set titles
    plt.title('Distribution of Companies in Germany by Circular Economy RE Strategies')

    # Save the map as an image
    logger.info(f"Saving map to {output_image}")
    plt.savefig(output_image, dpi=300)

    # Show the plot
    plt.show()
    logger.info("Map generation completed")