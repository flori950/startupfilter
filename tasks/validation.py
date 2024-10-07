import os
import json
import pandas as pd
from bigquery.client import BigQueryClient
from logger import Logger as logger
from openai.client import OpenAIClient
from openai.openai_requests_prompt import construct_prompt
from company_keywords.keywords import Keywords

def validate_columns(df, required_columns):
    """
    Validate if the required columns exist in the DataFrame.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    return True

def validate_strategy_code(strategy_code, strategy_dict):
    """
    Validate if the strategy code exists in the given strategy dictionary.
    """
    if strategy_code not in strategy_dict:
        logger.error(f"Strategy code {strategy_code} is not valid.")
        return False
    return True

def handle_row_error(row, error_message):
    """
    Logs an error when there is an issue with processing a row.
    """
    logger.error(f"Error processing row: {row}. Error: {error_message}")
    return "Error in OpenAI response"

def load_cache(cache_file='openai_cache.json'):
    """
    Loads the cache from a JSON file. If the file does not exist, it returns an empty dictionary.
    """
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache, cache_file='openai_cache.json'):
    """
    Saves the cache to a JSON file.
    """
    with open(cache_file, 'w') as f:
        json.dump(cache, f)

def get_cache_key(company_name, city, country, strategy_code):
    """
    Generate a unique cache key based on company name, city, country, and strategy code.
    """
    return f"{company_name}_{city}_{country}_{strategy_code}"

def process_csv_and_save(input_csv, output_csv, strategy_dict, openai_client, cache_file='openai_cache.json'):
    """
    Reads the categorized Crunchbase CSV, sends each entry to OpenAI, and adds the OpenAI response
    as a new column 'openai_answer'. Saves the new DataFrame to a new CSV, using caching.
    """
    logger.info(f"Loading data from {input_csv}")
    
    # Load the cache
    cache = load_cache(cache_file)
    
    # Read the input CSV
    df = pd.read_csv(input_csv)

    # Validate if required columns exist
    required_columns = ['Company_Name', 'City', 'Country', 'RE_Strategy', 'Short_Description']
    if not validate_columns(df, required_columns):
        return
    
    # Loop through each row and generate OpenAI responses
    openai_responses = []
    for _, row in df.iterrows():
        try:
            company_name = row['Company_Name']
            city = row['City']
            country = row['Country']
            strategy_code = row['RE_Strategy']
            short_description = row['Short_Description']

            # Validate strategy code
            if not validate_strategy_code(strategy_code, strategy_dict):
                openai_responses.append("Invalid strategy code")
                continue

            # Generate a unique cache key
            cache_key = get_cache_key(company_name, city, country, strategy_code)
            
            # Check if the result is already cached
            if cache_key in cache:
                logger.info(f"Using cached response for {company_name}")
                openai_responses.append(cache[cache_key])
                continue

            # Construct the OpenAI prompt
            prompt = construct_prompt(company_name, city, country, strategy_code, short_description)

            # Get OpenAI response
            logger.info(f"Sending request to OpenAI for {company_name} ({strategy_code})")
            response = openai_client.get_openai_response(prompt)

            # Extract the text part of the response
            openai_text = response['choices'][0]['text'].strip()

            # Store the response in the cache and append it to the results
            cache[cache_key] = openai_text
            openai_responses.append(openai_text)
        except Exception as e:
            openai_responses.append(handle_row_error(row, str(e)))

    # Add the OpenAI responses as a new column
    df['openai_answer'] = openai_responses

    # Save the updated DataFrame to the output CSV
    logger.info(f"Saving new CSV with OpenAI responses to {output_csv}")
    df.to_csv(output_csv, index=False)

    # Save the updated cache to the cache file
    save_cache(cache, cache_file)

    # Explicitly delete the DataFrame and clear memory
    del df
    logger.log("Validation job complete.")


def run_job(client: OpenAIClient, bqclient: BigQueryClient, upload=False):

    #TODO bigquery upload
    # Process the CSV and add the OpenAI responses
    input_csv = 'reporting/categorized_crunchbase_with_address.csv'
    output_csv = 'reporting/categorized_crunchbase_with_openai_responses.csv'
    re_strategies = Keywords.re_strategies_2

    process_csv_and_save(input_csv, output_csv, re_strategies, client)
