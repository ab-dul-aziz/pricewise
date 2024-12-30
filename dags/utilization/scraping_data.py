import os
import json
import base64
import pandas as pd
from time import sleep, perf_counter
from typing import Union
from dotenv import load_dotenv
from openai import OpenAI
from zyte_api import ZyteAPI
from pydantic import BaseModel
from multidict import CIMultiDict
from w3lib.encoding import html_to_unicode, resolve_encoding
import html_text

# ================== Load Configuration ==================

def load_env(env_path: str):
    """Load environment variables from .env file."""
    load_dotenv(env_path)
    zyte_api_key = os.getenv('ZYTE_API_KEY')
    openai_api_key = os.getenv('OPENAI_API_KEY')
    return zyte_api_key, openai_api_key


# ================== Encoding Helpers ==================

def auto_detect_encoding(body: bytes) -> Union[str, None]:
    """Auto-detect encoding for HTML body."""
    for encoding in ["utf8", "cp1252"]:
        try:
            body.decode(encoding)
        except UnicodeError:
            continue
        return resolve_encoding(encoding)

def bytes_to_html(body: bytes, headers: list[dict]) -> str:
    """Convert HTTP response bytes to HTML."""
    headers_dict = CIMultiDict([(h["name"], h["value"]) for h in headers])
    content_type = headers_dict.get("Content-Type")
    _, html = html_to_unicode(content_type, body, auto_detect_fun=auto_detect_encoding, default_encoding="utf8")
    return html

def extract_html(web_page: dict) -> str:
    """Extract HTML content from a Zyte web page response."""
    try:
        return web_page["html"]
    except KeyError:
        body = base64.b64decode(web_page["httpResponseBody"])
        headers = web_page["httpResponseHeaders"]
        return bytes_to_html(body, headers)


# ================== Scraping Helpers ==================

def initialize_clients(zyte_api_key: str, openai_api_key: str):
    """Initialize ZyteAPI and OpenAI clients."""
    client_zyte = ZyteAPI(api_key=zyte_api_key)
    client_openai = OpenAI(api_key=openai_api_key)
    return client_zyte, client_openai

def get_html_with_zapi(client_zyte: ZyteAPI, url: str, browser=False) -> Union[str, None]:
    """Retrieve HTML content of a page using ZyteAPI."""
    if browser:
        web_page = client_zyte.get({"url": url, "browserHtml": True})
        return web_page.get('browserHtml')
    else:
        web_page = client_zyte.get({"url": url, "httpResponseBody": True, "httpResponseHeaders": True})
        return extract_html(web_page)


# ================== GPT Extraction Helpers ==================

def extract_data_with_gpt(client_openai, text: str, schema: dict, model: str = "gpt-3.5-turbo", temperature: float = 0):
    """Use OpenAI GPT model to extract structured data from text."""
    instruction = f"""
    Extract data from the following text or web page:

    [TEXT START]
    {text}
    [TEXT END]

    The output must be a JSON object compliant with this schema:
    {json.dumps(schema, indent=4)}

    If a value is missing, set it to null.
    """.strip()

    completion = client_openai.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": instruction},
        ],
        temperature=temperature,
    )
    return completion


# ================== Main Scraping Process ==================

def ScrapingData():
    """Main scraping data process."""
    # Load environment variables
    dotenv_path = os.path.join(os.path.dirname(__file__), "/opt/airflow/data/.env")
    zyte_api_key, openai_api_key = load_env(dotenv_path)

    # Initialize clients
    client_zyte, client_openai = initialize_clients(zyte_api_key, openai_api_key)

    # Load property links
    link123 = pd.read_csv('/opt/airflow/data/link_properties.csv')

    # Define JSON schema
    schema = {
        "title": {"type": "string", "description": "The title of the house"},
        "description": {"type": "string", "description": "The description of the house"},
        "price": {"type": "number", "description": "The price of the house"},
        "address": {"type": "string", "description": "The address of the house"},
        "city": {"type": "string", "description": "The city of the house"},
        "land_size_m2": {"type": "number", "description": "The landsize (LT) without m2 of the house, if there is NaN fill 0"},
        "building_size_m2": {"type": "number", "description": "The buildingsize (LB) without m2 of the house, if there is NaN fill 0"},
        "bedroom": {"type": "number", "description": "The number of bedroom in the house, if there is NaN fill 0"},
        "bathroom": {"type": "number", "description": "The number of bathroom in the house, if there is NaN fill 0"},
        "garage": {"type": "number", "description": "The number of garage in the house, only the number and string that means number, if there is NaN fill 0"},
        "carport": {"type": "number", "description": "The number of carport in the house if there is NaN fill 0"},
        "property_type": {"type": "string", "description": "The type of the property, only if property_type = house"},
        "certificate": {"type": "string", "description": "The certificate of the house, if there is Null fill Not Specified"},
        "voltage_watt": {"type": "number", "description": "The voltage without watt of the house, if there is Null fill Not Specified"},
        "maid_bedroom": {"type": "number", "description": "The number of maid bedroom in the house, if there is NaN fill 0"},
        "maid_bathroom": {"type": "number", "description": "The number of maid bathroom in the house, if there is NaN fill 0"},
        "kitchen": {"type": "number", "description": "The number of kitchen in the house, if there is NaN fill 0"},
        "dining_room": {"type": "number", "description": "The number of dining room in the house, if there is NaN fill 0"},
        "living_room": {"type": "number", "description": "The number of living room in the house, if there is NaN fill 0"},
        "furniture": {"type": "string", "description": "The number of furniture in the house", "enum": ["Semi Furnished", "Furnished", "Unfurnished"]},
        "building_material": {"type": "string", "description": "The number of building material in the house"},
        "floor_material": {"type": "string", "description": "The number of building material in the house"},
        "floor_level": {"type": "number", "description": "The number of floor level in the house, if there is NaN fill 0"},
        "house_facing": {"type": "string", "description": "The number of face of the house", "enum": ["North", "South", "East", "West", "Southeast", "Southwest", "Northeast", "Northwest"]},
        "concept_and_style": {"type": "string", "description": "The concept and style of the house"},
        "view": {"type": "string", "description": "The view from the house"},
        "internet_access": {"type": "string", "description": "Whether the house has internet access"},
        "road_width": {"type": "string", "description": "The road width in front of the house"},
        "year_built": {"type": "number", "description": "The year the house was built"},
        "year_renovated": {"type": "number", "description": "The year the house was last renovated"},
        "water_source": {"type": "string", "description": "The water source for the house"},
        "corner_property": {"type": "boolean", "description": "Whether the house is a corner property (hook)"},
        "property_condition": {"type": "string", "description": "The condition of the property"},
        "ad_type": {"type": "string", "description": "The type of advertisement for the property"},
        "ad_id": {"type": "string", "description": "The ID of the advertisement"}
    }

    # Container for extracted data
    extracted_data = []

    # #! Uncomment to process the URLs
    # for idx, relative_url in enumerate(link123['property_url'][0:3]):
    #     url = f"https://www.rumah123.com{relative_url}"
    #     print(f"Processing {idx + 1}/{len(link123)}: {url}")
    #     try:
    #         # Fetch page HTML
    #         html = get_html_with_zapi(client_zyte, url, browser=False)
    #         text = html_text.extract_text(html, guess_layout=True)

    #         # Extract data using GPT
    #         completion = extract_data_with_gpt(client_openai, text, schema)
    #         extracted_data.append(completion.choices[0].message.content.strip())

    #         # Save progress periodically
    #         if idx % 2 == 0:
    #             with open("temp_data.json", "w") as f:
    #                 json.dump(extracted_data, f)
    #             print(f"Progress saved at index-{idx}")

    #     except Exception as e:
    #         print(f"Error at index-{idx}: {e}")

    #     sleep(1)

    # # Convert the list of strings to valid JSON and load them into a list
    # valid_json = []
    # for json_str in extracted_data:
    #     try:
    #         # Ensure proper JSON format (removing unwanted characters and handling malformed input)
    #         clean_json = json_str.replace('`', '').replace("\n", "").replace('json', '')
    #         valid_json.append(json.loads(clean_json))
    #     except json.JSONDecodeError:
    #         print(f"Error decoding JSON: {json_str}")

    # # Convert the list of valid JSON into a DataFrame
    # df = pd.DataFrame(valid_json)

    # # Final save to CSV
    # df.to_csv('/opt/airflow/data/Property_Scraping_tes.csv', index=False)
    print("Data has been saved to Property_Scraping.csv.")


# ================== Entry Point ==================

if __name__ == "__main__":
    ScrapingData()
