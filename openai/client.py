import requests
import openai
from helpers.decorators import retry
from logger import Logger as logger

class AccessError(Exception):
    pass

class OpenAIClient:
    """ Open AI Client """
    max_retries = 5
    retry_delay = 10

    def __init__(self, api_key, base_url):
        """
        Initializes the OpenAI Client.

        Args:
            api_key (str): The API key for OpenAI.
            base_url (str): The OpenAI base URL.
        """
        try:
            self.API_KEY = api_key
            self.QUERY_URL = base_url
            openai.api_key = self.API_KEY  # Initialize OpenAI with API key

            # Test API connectivity during initialization
            if not self.test_api_connectivity():
                raise ConnectionError("OpenAI API is not reachable")
        except Exception as e:
            logger.error(f"Problem with OpenAI credentials: {e}")
            raise e

    def test_api_connectivity(self):
        """
        Test API connectivity by making a test request to the OpenAI API.
        """
        try:
            # Make a test request to the API with the payload
            response = requests.get(f"{self.QUERY_URL}models", headers={"Authorization": f"Bearer {self.API_KEY}"})
            if response.status_code == 200:
                logger.success("OpenAI API is reachable")
                return True
            else:
                logger.error(f"OpenAI API is not reachable. Status code: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error testing API connectivity: {e}")
            return False

    @retry(max_retries, retry_delay)
    def get_data(self, prompt, model="text-davinci-003", max_tokens=100, temperature=0.7):
        """
        Sends a request to the OpenAI API to get a response based on the given prompt.

        Args:
            prompt (str): The prompt to send to the API.
            model (str): The model to use for the request.
            max_tokens (int): Maximum number of tokens in the response.
            temperature (float): Sampling temperature to control the creativity.

        Returns:
            dict: The JSON response from the OpenAI API.
        """
        try:
            response = openai.Completion.create(
                engine=model,
                prompt=prompt,
                max_tokens=max_tokens,
                temperature=temperature
            )
            return response.to_dict()  # Return the full response as a dictionary
        except Exception as e:
            logger.error(f"Error making OpenAI request: {e}")
            raise AccessError("Failed to retrieve data from OpenAI API") from e