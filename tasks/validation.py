import os
import json
import pandas as pd
from bigquery.client import BigQueryClient
from logger import Logger as logger
from company_keywords.keywords import Keywords
from openai_request.client import OpenAIClient
from openai_request.openai_requests_prompt import construct_prompt
from tasks.mapping import generate_germany_map

def run_job(client: OpenAIClient, bqclient: BigQueryClient, upload=False):

    #TODO bigquery upload
    # Process the CSV and add the OpenAI responses
    input_csv = 'reporting/categorized_crunchbase_with_address.csv'
    output_csv = 'reporting/categorized_crunchbase_with_openai_responses.csv'
    re_strategies = Keywords.re_strategies

    process_csv_and_save(input_csv, output_csv, re_strategies, client)

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

def get_cache_key(company_name, city, country, strategy_code):
    """
    Generate a unique cache key based on company name, city, country, and strategy code.
    """
    return f"{company_name}_{city}_{country}_{strategy_code}"

def process_csv_and_save(input_csv, output_csv, strategy_dict, openai_client, cache_file='openai_cache.json'):
    """
    Reads the categorized Crunchbase CSV, sends each entry to OpenAI, and adds the strategy code and term or a disagreement message
    as a new column 'openai_answer'. Saves the new DataFrame to a new CSV, using caching.
    """
    logger.info(f"Loading data from {input_csv}")
    
    # Load the cache
    cache = load_cache(cache_file)
    
    # Read the input CSV
    df = pd.read_csv(input_csv)

    # Validate if required columns exist
    required_columns = ['Company_Name', 'City', 'Country', 'RE_Strategy_Codes', 'RE_Strategy_Names', 'Short_Description']
    if not validate_columns(df, required_columns):
        return
    
    # Loop through each row and generate OpenAI responses
    openai_responses = []
    for _, row in df.iterrows():
        try:
            company_name = row['Company_Name']
            city = row['City']
            country = row['Country']
            strategy_codes = row['RE_Strategy_Codes'].split(", ")
            short_description = row['Short_Description']

            # Iterate over each strategy code
            strategy_code_and_term = []
            for strategy_code in strategy_codes:
                # Validate strategy code
                if not validate_strategy_code(strategy_code, strategy_dict):
                    strategy_code_and_term.append(f"Invalid strategy code: {strategy_code}")
                    continue

                # Generate a unique cache key based on company and strategy
                cache_key = get_cache_key(company_name, city, country, strategy_code)
                
                # Check if the result is already cached
                if cache_key in cache:
                    logger.info(f"Using cached response for {company_name} ({strategy_code})")
                    strategy_code_and_term.append(cache[cache_key])
                    continue

                # Construct the OpenAI prompt for each strategy
                messages = construct_prompt(company_name, city, country, strategy_code, short_description)

                # Get OpenAI response
                logger.info(f"Sending request to OpenAI for {company_name} ({strategy_code})")
                response = openai_client.get_openai_response(messages)

                # Add the response (either "R0: Refuse" or "Disagree: explanation")
                strategy_code_and_term.append(response)

                # Cache the response
                cache[cache_key] = response

                # Save cache after every row
                save_cache(cache, cache_file)

            # Join the multiple responses into a single string for each company
            openai_responses.append(", ".join(strategy_code_and_term))

        except Exception as e:
            openai_responses.append(handle_row_error(row, str(e)))

    # Add the responses as a new column
    df['openai_answer'] = openai_responses

    # Save the updated DataFrame to the output CSV
    logger.info(f"Saving new CSV with OpenAI responses to {output_csv}")
    df.to_csv(output_csv, index=False)

    # Call the function to generate the map after saving the CSV
    # logger.log("Generating map based on categorized data")
    # generate_germany_map(output_csv, "reporting/germany_re_strategy_map_ai_response.png")

    # Explicitly delete the DataFrame and clear memory
    del df
    logger.log("Validation job complete.")


