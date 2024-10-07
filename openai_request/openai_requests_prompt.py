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
        list: A list of messages with system and user roles for OpenAI API.
    """
    strategy = Keywords.re_strategies_2.get(strategy_code)
    if not strategy:
        raise ValueError(f"Strategy code {strategy_code} not found in Keywords")

    # Return messages for the chat-based OpenAI API
    messages = [
        {
            "role": "system",
            "content": "You are a helpful assistant that provides only concise, structured answers."
        },
        {
            "role": "user",
            "content": (
                f"Please only provide the circular economy strategy code and name for the company '{company_name}' "
                f"located in {city}, {country}. "
                f"The company description is '{short_description}'. "
                f"The strategy '{strategy_code}' is '{strategy['name']}'.\n"
                f"Output format: R8: Recycle. Do not include any explanation or additional information."
            )
        }
    ]
    
    return messages
