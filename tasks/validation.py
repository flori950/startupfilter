import os
import pandas as pd
from openai.client import OpenAIClient
from logger import Logger as logger
from run import CONFIG
from startup_keywords.keywords import Keywords
from openai.openai_requests_prompt import Prompts

def construct_prompt(description, definitions):
    """
    Constructs the prompt for the OpenAI request using the 9R definitions.

    Args:
        description (str): The description of the company.
        definitions (str): The 9R definitions formatted as a single string.

    Returns:
        str: The constructed prompt to be sent to OpenAI.
    """
    # Insert the definitions into the prompt template
    prompt = ''.join(Prompts.prompt_template[:1]) + definitions + ''.join(Prompts.prompt_template[1:])
    # Append the company description
    prompt += description + "\n"
    return prompt

def categorize_startup(row, client):
    """
    Categorizes a startup based on its description by matching it with the 9R categories using OpenAI.
    
    Args:
        row (pd.Series): A row from the DataFrame containing company data.
        client (OpenAIClient): The OpenAI client to make requests.
    
    Returns:
        pd.Series: Updated row with the 9R category, confidence percentage, and AI answer.
    """
    description = row['Description']
    company_name = row['Company_Name']
    
    logger.info(f"Processing company: {company_name}")

    # Construct the prompt using the helper function from openai_request.py
    prompt = construct_prompt(description)

    # Make the OpenAI request
    try:
        response = client.get_data(prompt)

        if response:
            answer = response['choices'][0]['text'].strip()
            # Check if the answer matches any of the 9R categories
            for category in Keywords.nine_r_descriptions.keys():
                if category in answer:
                    return pd.Series([category, 100, answer])  # Returning the category, confidence, and AI answer
        return pd.Series(["Uncategorized", 0, response['choices'][0]['text'].strip()])  # If no match found, still return AI answer
    except Exception as e:
        logger.error(f"Error processing company {company_name}: {e}")
        return pd.Series(["Uncategorized", 0, "Error in AI response"])


def run_job(openaiclient: OpenAIClient):
    # Define the path to the input CSV relative to the current script location
    csv_path = os.path.join(os.path.dirname(__file__), "../reporting/categorized_crunchbase.csv")

    # Fetch data from Crunchbase
    logger.log("Fetching data from reporting")
    df = pd.read_csv(csv_path)

    # Apply categorization
    logger.log("Categorizing startups based on their descriptions")
    df[['9R_Category', '9R_Percentage', 'AI_Answer']] = df.apply(categorize_startup, axis=1, client=openaiclient)

    # Filter out rows where the category is 'Uncategorized'
    df_filtered = df[df['9R_Category'] != 'Uncategorized']

    # Select only the necessary columns, including the new 'AI_Answer' column
    df_filtered = df_filtered[['Company_Name', 'Description', '9R_Category', 'BMT_Category', '9R_Percentage', 'AI_Answer']]

    # Save categorized data as CSV
    output_path = os.path.join(os.path.dirname(__file__), "../reporting/categorized_crunchbase.csv")
    logger.log(f"Saving categorized data to {output_path}")
    df_filtered.to_csv(output_path, index=False)

    # Cleanup
    del df
    del df_filtered

if __name__ == "__main__":
    client = OpenAIClient(CONFIG.OPENAI_API_KEY, CONFIG.OPENAI_BASE_URL)
    run_job(client)
