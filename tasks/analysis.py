import os
import pandas as pd
from startup_keywords.keywords import Keywords
from logger import Logger as logger

def run_job():
    # Define the path to crunchbase.csv relative to the current script location
    csv_path = os.path.join(os.path.dirname(__file__), "../reporting/crunchbase.csv")

    # Fetch data from Crunchbase
    logger.log("Fetching data from reporting")
    df = pd.read_csv(csv_path)

    # Apply categorization and percentage calculation
    logger.log("Categorizing startups based on their short descriptions")
    df[['Company_Name', 'Description', '9R_Category', 'BMT_Category', 
        '9R_Percentage', 'BMT_Percentage']] = df.apply(categorize_startup, axis=1)

    # Filter out rows where both categories are 'Uncategorized'
    df_filtered = df[(df['9R_Category'] != 'Uncategorized') | (df['BMT_Category'] != 'Uncategorized')]

    # Select only the necessary columns
    df_filtered = df_filtered[['Company_Name', 'Description', '9R_Category', 'BMT_Category', '9R_Percentage', 'BMT_Percentage']]

    # Save categorized data as CSV
    logger.log("Saving categorized data as csv")
    df_filtered.to_csv("reporting/categorized_crunchbase.csv", index=False)

    # Delete dataframes to free up memory
    del df
    del df_filtered


def categorize_startup(row):
    # Extract relevant columns
    company_name = row['Name']
    description = row['Short_Description']

    # Initialize category variables
    category_9r = "Uncategorized"
    category_bmt = "Uncategorized"
    
    # Initialize percentage variables
    nine_r_percentage = 0
    business_model_percentage = 0

    # Handle NaN or missing values
    if pd.isna(company_name):
        company_name = ""
    if pd.isna(description):
        description = ""

    # Ensure description is a string
    description = str(description).lower()

    # Count total number of words in description
    total_words = len(description.split())

    # 9R Framework categorization and percentage calculation
    for category, keyword_list in Keywords.nine_r_keywords.items():
        count = sum(keyword in description for keyword in keyword_list)
        if total_words > 0:
            category_percentage = count / total_words * 100
            if category_percentage > nine_r_percentage:
                nine_r_percentage = category_percentage
                category_9r = category

    # Business Model Types categorization and percentage calculation
    for category, keyword_list in Keywords.business_model_keywords.items():
        count = sum(keyword in description for keyword in keyword_list)
        if total_words > 0:
            category_percentage = count / total_words * 100
            if category_percentage > business_model_percentage:
                business_model_percentage = category_percentage
                category_bmt = category

    # Create a pandas Series with all relevant information
    return pd.Series([company_name, description, category_9r, category_bmt, 
                      nine_r_percentage, business_model_percentage])


# Example usage
if __name__ == "__main__":
    run_job()
