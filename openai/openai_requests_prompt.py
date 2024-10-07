# openai_request_prompt.py
from company_keywords.keywords import Keywords

def construct_prompt(company_name, city, country, strategy_code, short_description):
    """
    Constructs the OpenAI prompt based on the company name, city, country, RE strategy, and company description.

    Args:
        company_name (str): The name of the company.
        city (str): The city of the company.
        country (str): The country of the company.
        strategy_code (str): The RE strategy code (R0, R1, etc.).
        short_description (str): The short description of the company.

    Returns:
        str: The constructed prompt for OpenAI.
    """
    strategy = Keywords.re_strategies_2.get(strategy_code)
    if not strategy:
        raise ValueError(f"Strategy code {strategy_code} not found in Keywords")

    prompt = (
        f"Please analyze how the company '{company_name}' located in {city}, {country} "
        f"with the following description: '{short_description}', "
        f"can apply the circular economy strategy '{strategy['name']}' which is defined as: "
        f"'{strategy['definition']}'."
    )
    
    return prompt
