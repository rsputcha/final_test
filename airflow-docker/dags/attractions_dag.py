from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import asyncio
import csv
import time
import os
import logging
import pandas as pd
import boto3
import requests
import snowflake.connector
from io import StringIO
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from dotenv import load_dotenv
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

# Default args to prevent duplicate runs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,  
    'max_active_runs': 1  
}

# S3 and Snowflake credentials
aws_key = os.getenv("AWS_ACCESS_KEY")
aws_secret = os.getenv("AWS_SECRET_KEY")
aws_region = os.getenv("AWS_REGION")
google_maps_api_key = os.getenv("GOOGLE_MAPS_API_KEY")
s3_bucket = os.getenv("S3_BUCKET", "bigdatafinal2025")

SNOWFLAKE_CREDENTIALS = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE")
}

S3_STAGE = os.getenv("SNOWFLAKE_STAGE")

# File paths
DATA_DIR = "/tmp/triphobo_data"
os.makedirs(DATA_DIR, exist_ok=True)
RAW_ATTRACTIONS_PATH = f"{DATA_DIR}/multi_city_attractions.csv"
CLEAN_ATTRACTIONS_PATH = f"{DATA_DIR}/cleaned_attractions.csv"
GEOCODED_ATTRACTIONS_PATH = f"{DATA_DIR}/attractions_with_coords.csv"
CHECKPOINT_PATH = f"{DATA_DIR}/scrape_checkpoint.json"

# S3 paths
RAW_ATTRACTIONS_S3_KEY = "raw/multi_city_attractions.csv"
CLEAN_ATTRACTIONS_S3_KEY = "clean/cleaned_attractions.csv"
GEOCODED_ATTRACTIONS_S3_KEY = "clean/attractions_with_coords.csv"

# Snowflake tables
SNOWFLAKE_ATTRACTIONS_TABLE = "TRIPHOBO_ATTRACTIONS"

# City data
CITY_URLS = [
    "https://www.triphobo.com/places/new-york-city-united-states/things-to-do",
    "https://www.triphobo.com/places/san-francisco-united-states/things-to-do",
    "https://www.triphobo.com/places/chicago-united-states/things-to-do",
    "https://www.triphobo.com/places/las-vegas-united-states/things-to-do",
    "https://www.triphobo.com/places/los-angeles-united-states/things-to-do",  
    "https://www.triphobo.com/places/seattle-united-states/things-to-do"
]

CITY_CENTERS = {
    'new york city': 'New York City, NY, USA',
    'chicago': 'Chicago, IL, USA',
    'san francisco': 'San Francisco, CA, USA',
    'seattle': 'Seattle, WA, USA',
    'las vegas': 'Las Vegas, NV, USA',
    'los angeles': 'Los Angeles, CA, USA'
}

#================ SCRAPING FUNCTIONS ===================

BASE_URL = "https://www.triphobo.com"

async def extract_readmore_links(page, url):
    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(3000)
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        links = []
        for div in soup.select("div.attr-tour-cont-top-rhs-btm a.read-more"):
            href = div.get("href")
            if href:
                full_url = href if href.startswith("http") else BASE_URL + href
                links.append(full_url)
        return links
    except Exception as e:
        logging.error(f"Failed to extract read more links from {url}: {e}")
        return []

def extract_section_by_keyword(soup_section, keyword):
    try:
        for header in soup_section.find_all(["h2", "label"]):
            if header and keyword.lower() in header.get_text(strip=True).lower():
                ul = header.find_next("ul")
                if ul:
                    return "\n".join(li.get_text(strip=True) for li in ul.find_all("li"))
        return "N/A"
    except Exception as e:
        logging.error(f"Error extracting section by keyword {keyword}: {e}")
        return "N/A"

async def extract_attraction_details(page, url):
    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(3000)
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        cms_div = soup.select_one("#cms-data")
        if not cms_div:
            return {"URL": url, "Description": "N/A", "Travel Tips": "N/A", "Ticket Details": "N/A",
                    "Hours": "N/A", "How to Reach": "N/A", "Restaurants Nearby": "N/A", "Image": "N/A"}

        description = "\n".join([p.get_text(strip=True) for p in cms_div.find_all("p") if p.get_text(strip=True)])
        
        # Try to find the image
        image_url = "N/A"
        try:
            img_meta = soup.select_one('meta[property="og:image"]')
            if img_meta and img_meta.get("content"):
                image_url = img_meta.get("content")
        except Exception as img_err:
            logging.error(f"Error extracting image from {url}: {img_err}")

        return {
            "URL": url,
            "Description": description,
            "Travel Tips": extract_section_by_keyword(cms_div, "travel tips"),
            "Ticket Details": extract_section_by_keyword(cms_div, "ticket details"),
            "Hours": extract_section_by_keyword(cms_div, "hours"),
            "How to Reach": extract_section_by_keyword(cms_div, "how to reach"),
            "Restaurants Nearby": extract_section_by_keyword(cms_div, "restaurants"),
            "Image": image_url  
        }
    except Exception as e:
        logging.error(f"Failed to extract details from {url}: {e}")
        return {"URL": url, "Error": str(e), "Image": "N/A"}

def save_checkpoint(city_index, page_index, completed_cities):
    checkpoint = {
        "city_index": city_index,
        "page_index": page_index,
        "completed_cities": completed_cities
    }
    
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump(checkpoint, f)
        
    logging.info(f"Saved checkpoint: City {city_index}, Page {page_index}, Completed cities: {completed_cities}")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        try:
            with open(CHECKPOINT_PATH, 'r') as f:
                checkpoint = json.load(f)
            logging.info(f"Loaded checkpoint: City {checkpoint['city_index']}, Page {checkpoint['page_index']}, Completed cities: {checkpoint['completed_cities']}")
            return checkpoint
        except Exception as e:
            logging.error(f"Error loading checkpoint: {e}")
    
    # Default checkpoint (start from beginning)
    return {"city_index": 0, "page_index": 1, "completed_cities": []}

async def scrape_city_async(output_path, city_index, start_page=1, max_pages=10):
    all_data = []
    if os.path.exists(output_path):
        try:
            existing_df = pd.read_csv(output_path)
            all_data = existing_df.to_dict('records')
            logging.info(f"Loaded {len(all_data)} existing records from {output_path}")
            
            existing_urls = set(existing_df['URL'].tolist())
            logging.info(f"Found {len(existing_urls)} existing URLs in the dataset")
        except Exception as e:
            logging.error(f"Error loading existing data: {e}")
            existing_urls = set()
    else:
        existing_urls = set()

    if city_index >= len(CITY_URLS):
        logging.info(f"City index {city_index} out of range, all cities completed")
        return all_data, True
        
    city_url = CITY_URLS[city_index]
    city_name = city_url.split("/")[4].replace("-", " ").title()
    logging.info(f"Scraping city {city_index+1}/{len(CITY_URLS)}: {city_name}")
    city_data_count = 0
    
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for page_num in range(start_page, max_pages + 1):
                paginated_url = f"{city_url}?page={page_num}"
                logging.info(f"Scraping page {page_num}/{max_pages}: {paginated_url}")
                
                try:
                    read_more_links = await extract_readmore_links(page, paginated_url)
                    logging.info(f"Found {len(read_more_links)} attraction links")
                    
                    if not read_more_links:
                        logging.info(f"No more links found on page {page_num}, moving to next city")
                        break
                        
                    # Process attractions in smaller batches
                    batch_size = 5
                    for batch_idx in range(0, len(read_more_links), batch_size):
                        batch_links = read_more_links[batch_idx:batch_idx+batch_size]
                        
                        for idx, link in enumerate(batch_links):
                            if link in existing_urls:
                                logging.info(f"Skipping already scraped URL: {link}")
                                continue
                                
                            try:
                                logging.info(f"Scraping place {batch_idx+idx+1}/{len(read_more_links)}: {link}")
                                data = await extract_attraction_details(page, link)
                                data["City"] = city_name
                                all_data.append(data)
                                existing_urls.add(link)
                                city_data_count += 1
                                
                                # Save intermediate results frequently
                                if len(all_data) % 5 == 0:
                                    normalized_data = normalize_data(all_data)
                                    df = pd.DataFrame(normalized_data)
                                    df.to_csv(output_path, index=False)
                                    save_checkpoint(city_index, page_num, [])
                                    
                            except Exception as e:
                                logging.error(f"Failed to scrape {link}: {e}")
                                all_data.append({
                                    "URL": link, 
                                    "City": city_name, 
                                    "Error": str(e),
                                    "Description": "N/A",
                                    "Travel Tips": "N/A",
                                    "Ticket Details": "N/A",
                                    "Hours": "N/A",
                                    "How to Reach": "N/A",
                                    "Restaurants Nearby": "N/A",
                                    "Image": "N/A"
                                })
                        
                        await asyncio.sleep(2)
                        
                    # Save checkpoint after each page
                    normalized_data = normalize_data(all_data)
                    df = pd.DataFrame(normalized_data)
                    df.to_csv(output_path, index=False)
                    save_checkpoint(city_index, page_num + 1, [])
                    
                except Exception as e:
                    logging.error(f"Failed to process page {page_num} of {city_name}: {e}")
                    # Still save checkpoint to avoid reprocessing pages
                    save_checkpoint(city_index, page_num + 1, [])
            
            await browser.close()
            logging.info(f"Completed scraping {city_name}, found {city_data_count} new attractions")
            
            # All pages done, prepare for next city
            normalized_data = normalize_data(all_data)
            df = pd.DataFrame(normalized_data)
            df.to_csv(output_path, index=False)
            
            # Mark city as completed and move to next city
            completed = city_index + 1 >= len(CITY_URLS)
            save_checkpoint(city_index + 1, 1, [city_name])
            
            return all_data, completed
            
        except Exception as e:
            logging.error(f"Error during city scraping: {e}")
            df = pd.DataFrame(all_data)
            df.to_csv(output_path, index=False)
            return all_data, False

def normalize_data(data_list):
    all_keys = set()
    for item in data_list:
        all_keys.update(item.keys())
    
    # Ensure all items have all keys
    normalized_data = []
    for item in data_list:
        normalized_item = {}
        for key in all_keys:
            normalized_item[key] = item.get(key, "N/A")
        normalized_data.append(normalized_item)
    
    return normalized_data

def scrape_city(output_path, city_index=None, start_page=None):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        if city_index is None or start_page is None:
            checkpoint = load_checkpoint()
            city_index = checkpoint["city_index"]
            start_page = checkpoint["page_index"]
        
        # Scrape the city
        all_data, completed = asyncio.run(scrape_city_async(output_path, city_index, start_page))
        
        if completed:
            logging.info("All cities have been scraped!")
        else:
            logging.info(f"City {city_index} scraped, more cities pending")
            
        return len(all_data)
        
    except Exception as e:
        logging.error(f"Error in scrape_city: {e}")
        raise

def scrape_attractions_all_cities(output_path):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        total_attractions = 0
        
        for i in range(len(CITY_URLS)):
            result = scrape_city(output_path, city_index=i, start_page=1)
            total_attractions += result
            logging.info(f"Completed city {i+1}/{len(CITY_URLS)}, total attractions: {total_attractions}")
            
        return total_attractions
    
    except Exception as e:
        logging.error(f"Error processing all cities: {e}")
        raise

def scrape_attractions(output_path):
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        checkpoint = load_checkpoint()
        city_index = checkpoint["city_index"]
        start_page = checkpoint["page_index"]
        
        if city_index >= len(CITY_URLS):
            logging.info("All cities have been scraped already!")
            save_checkpoint(0, 1, [])
            city_index = 0
            start_page = 1
            
        # Scrape one city at a time
        result = scrape_city(output_path, city_index, start_page)
        logging.info(f"Attraction scraping completed for city {city_index}, saved {result} attractions to {output_path}")
        return result
        
    except Exception as e:
        logging.error(f"Error running the scraping process: {e}")
        raise

def reset_checkpoint():
    """Reset the checkpoint to start from the beginning"""
    save_checkpoint(0, 1, [])
    return "Checkpoint reset to start with first city"

def clean_attractions_csv(input_path, output_path):
    try:
        df = pd.read_csv(input_path)
        df.replace(["N/A", "", " ", "nan", "NaN"], pd.NA, inplace=True)
        df.drop_duplicates(inplace=True)

        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].str.strip() if hasattr(df[col], 'str') else df[col]

        if 'Description' in df.columns:
            df["Description"] = df["Description"].str.replace(r"\s+", " ", regex=True) if hasattr(df["Description"], 'str') else df["Description"] 
            df["Short Description"] = df["Description"].apply(lambda x: x[:500] + "..." if isinstance(x, str) and len(x) > 500 else x)

        required_columns = ["URL", "Description", "Travel Tips", "Ticket Details", "Hours", 
                           "How to Reach", "Restaurants Nearby", "Image", "City"]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = "Not Provided"

        df.fillna("Not Provided", inplace=True)

        df.to_csv(output_path, index=False)
        logging.info(f"Cleaned attraction data saved to {output_path}")

    except Exception as e:
        logging.error(f"Error cleaning attractions CSV: {e}", exc_info=True)
        raise


def extract_place_from_url(url):
    try:
        path = urlparse(url).path
        last_part = path.strip('/').split('/')[-1]
        return ' '.join(word.capitalize() for word in last_part.replace('-', ' ').split())
    except Exception as e:
        logging.warning(f"Failed to extract place from URL: {url} - {e}")
        return "Unknown Place"

def extract_city_name(city_string):
    if not city_string:
        return None
    parts = city_string.lower().split()
    for city in CITY_CENTERS.keys():
        if city.split()[0] in parts:
            return city
    return parts[0]

def geocode_place(place_name, city_name):
    search_query = f"{place_name}, {CITY_CENTERS.get(city_name, '')}" if city_name in CITY_CENTERS else place_name
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": search_query, "key": google_maps_api_key}

    try:
        response = requests.get(url, params=params)
        data = response.json()

        if data['status'] == 'OK' and data['results']:
            location = data['results'][0]['geometry']['location']
            return {
                'lat': location['lat'],
                'lng': location['lng'],
                'formatted_address': data['results'][0]['formatted_address'],
                'search_query': search_query
            }
        else:
            logging.warning(f"No results for: {search_query}. Status: {data['status']}")
            return {'lat': '', 'lng': '', 'formatted_address': '', 'search_query': search_query}
    except Exception as e:
        logging.error(f"Error geocoding '{search_query}': {e}", exc_info=True)
        return {'lat': '', 'lng': '', 'formatted_address': '', 'search_query': search_query}

def add_coordinates_to_csv(input_path, output_path, batch_size=50):
    try:
        with open(input_path, 'r', encoding='utf-8') as input_file:
            reader = csv.DictReader(input_file)
            all_rows = list(reader)

        total_rows = len(all_rows)
        logging.info(f"Total attractions to process: {total_rows}")

        try:
            with open(output_path, 'r', encoding='utf-8') as test_file:
                processed_count = sum(1 for _ in csv.DictReader(test_file))
                logging.info(f"Resuming from existing output file with {processed_count} rows")
                start_index = processed_count
                append_mode = True
        except FileNotFoundError:
            start_index = 0
            append_mode = False

        all_fields = list(all_rows[0].keys())
        # Add new fields that will be added
        output_fields = all_fields + ['Latitude', 'Longitude', 'PlaceName', 'FormattedAddress', 'SearchQuery']
        
        mode = 'a' if append_mode else 'w'

        with open(output_path, mode, encoding='utf-8', newline='') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=output_fields)
            if not append_mode:
                writer.writeheader()

            for i in range(start_index, total_rows, batch_size):
                batch_end = min(i + batch_size, total_rows)
                logging.info(f"Processing batch {i // batch_size + 1}: rows {i + 1} to {batch_end}")

                for idx in range(i, batch_end):
                    row = all_rows[idx]
                    place_name = extract_place_from_url(row.get('URL', ''))
                    city_name = extract_city_name(row.get('City', ''))

                    logging.info(f"  ({idx + 1}/{total_rows}) Geocoding: {place_name}")
                    geo_results = geocode_place(place_name, city_name)

                    updated_row = row.copy()
                    updated_row['Latitude'] = geo_results['lat']
                    updated_row['Longitude'] = geo_results['lng']
                    updated_row['PlaceName'] = place_name
                    updated_row['FormattedAddress'] = geo_results.get('formatted_address', '')
                    updated_row['SearchQuery'] = geo_results.get('search_query', '')

                    writer.writerow(updated_row)
                    time.sleep(0.1)  # Rate limiting

                logging.info(f"Completed batch {i // batch_size + 1}. Sleeping before next batch...")
                time.sleep(2)  # Sleep between batches
                output_file.flush()

        logging.info(f"Successfully processed all {total_rows} attractions")
        logging.info(f"Output saved to: {output_path}")

    except Exception as e:
        logging.error(f"Error processing CSV: {e}", exc_info=True)
        raise


def upload_to_s3(file_path, bucket, s3_key):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=aws_region
    )
    
    try:
        s3.upload_file(file_path, bucket, s3_key)
        logging.info(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")
    except Exception as e:
        logging.error(f"Failed to upload {file_path} to s3://{bucket}/{s3_key}: {e}")
        raise


def load_from_s3_to_snowflake(s3_bucket, s3_key, table_name):
    try:
        # S3 Client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=aws_region
        )

        logging.info(f"Downloading file from s3://{s3_bucket}/{s3_key}")
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        df = pd.read_csv(StringIO(csv_content), on_bad_lines='skip')
        logging.info(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
        
        logging.info(f"CSV columns: {df.columns.tolist()}")
        if len(df) > 0:
            logging.info(f"First row sample: {df.iloc[0].to_dict()}")

        required_columns = [
            "URL", "Description", "Travel Tips", "Ticket Details", "Hours", 
            "How to Reach", "Restaurants Nearby", "City", "Short Description",
            "Latitude", "Longitude", "PlaceName", "FormattedAddress"
        ]
        
        for col in required_columns:
            if col not in df.columns:
                logging.info(f"Adding missing column: {col}")
                df[col] = ""
                
        if 'Image' not in df.columns:
            logging.info("Adding missing Image column")
            df['Image'] = ''

        # Fix column order to match Snowflake table
        columns_order = [
            "URL", "Description", "Travel Tips", "Ticket Details", "Hours", 
            "How to Reach", "Restaurants Nearby", "Image", "City", "Short Description",
            "Latitude", "Longitude", "PlaceName", "FormattedAddress"
        ]
        
        # Create a new DataFrame with only the needed columns in the right order
        new_df = pd.DataFrame()
        for col in columns_order:
            if col in df.columns:
                new_df[col] = df[col]
            else:
                new_df[col] = ""
        
        temp_file = "/tmp/attractions_with_coords_fixed.csv"
        new_df.to_csv(temp_file, index=False)
        
        file_size = os.path.getsize(temp_file)
        logging.info(f"Created temporary CSV file: {temp_file}, size: {file_size} bytes")

        # Connect to Snowflake
        conn = snowflake.connector.connect(**SNOWFLAKE_CREDENTIALS)
        cursor = conn.cursor()

        logging.info(f"Creating table {table_name} with predefined schema...")
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {table_name} (
            URL VARCHAR(16777216),
            DESCRIPTION VARCHAR(16777216),
            "Travel Tips" VARCHAR(16777216),
            "Ticket Details" VARCHAR(16777216),
            HOURS VARCHAR(16777216),
            "How to Reach" VARCHAR(16777216),
            "Restaurants Nearby" VARCHAR(16777216),
            IMAGE VARCHAR(16777216),
            CITY VARCHAR(16777216),
            "Short Description" VARCHAR(16777216),
            LATITUDE NUMBER(38,14),
            LONGITUDE NUMBER(38,14),
            PLACENAME VARCHAR(16777216),
            FORMATTEDADDRESS VARCHAR(16777216)
        )
        """
        cursor.execute(create_table_sql)

        logging.info("Uploading file to Snowflake stage...")
        cursor.execute(f"PUT file://{temp_file} @{S3_STAGE} OVERWRITE = TRUE")
        file_name = os.path.basename(temp_file)
        
        copy_sql = f"""
            COPY INTO {table_name} (
                URL, 
                DESCRIPTION,
                "Travel Tips",
                "Ticket Details",
                HOURS,
                "How to Reach",
                "Restaurants Nearby",
                IMAGE,
                CITY,
                "Short Description",
                LATITUDE,
                LONGITUDE,
                PLACENAME,
                FORMATTEDADDRESS
            )
            FROM @{S3_STAGE}/{file_name}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
                COMPRESSION = 'AUTO'
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                EMPTY_FIELD_AS_NULL = TRUE
                REPLACE_INVALID_CHARACTERS = TRUE
            )
            ON_ERROR = 'CONTINUE'
        """

        logging.info("Copying data into Snowflake table...")
        copy_result = cursor.execute(copy_sql).fetchall()
        logging.info(f"Copy command result: {copy_result}")
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logging.info(f"Snowflake table row count: {count}")
        
        # Verify data matches expectations
        expected_count = len(new_df)
        if count != expected_count:
            logging.warning(f"Row count mismatch! Expected {expected_count}, got {count}")
            
        cursor.execute(f"DESC TABLE {table_name}")
        columns = cursor.fetchall()
        logging.info(f"Table columns: {[col[0] for col in columns]}")

        cursor.close()
        conn.close()
        
        try:
            os.remove(temp_file)
        except:
            pass

    except Exception as e:
        logging.error(f"Error loading to Snowflake: {e}")
        raise

def process_next_city():
    checkpoint = load_checkpoint()
    city_index = checkpoint["city_index"]
    
    if city_index >= len(CITY_URLS):
        # Reset to start with the first city
        save_checkpoint(0, 1, [])
        return "All cities processed, resetting to first city"
    
    result = scrape_city(RAW_ATTRACTIONS_PATH, city_index, checkpoint["page_index"])
    return f"Processed city {city_index} ({CITY_URLS[city_index].split('/')[4]}), found {result} attractions"

def process_all():
    try:
        process_next_city()

        clean_attractions_csv(RAW_ATTRACTIONS_PATH, CLEAN_ATTRACTIONS_PATH)
        
        add_coordinates_to_csv(CLEAN_ATTRACTIONS_PATH, GEOCODED_ATTRACTIONS_PATH)
        
        upload_to_s3(GEOCODED_ATTRACTIONS_PATH, s3_bucket, GEOCODED_ATTRACTIONS_S3_KEY)
        
        load_from_s3_to_snowflake(s3_bucket, GEOCODED_ATTRACTIONS_S3_KEY, SNOWFLAKE_ATTRACTIONS_TABLE)
        
        return "Complete processing pipeline executed successfully"
    
    except Exception as e:
        logging.error(f"Error in process_all: {e}")
        raise


with DAG(
    dag_id="triphobo_attractions_scrape",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  
    catchup=False,
    max_active_runs=1,  
    tags=["triphobo", "tourism", "scrape"]
) as dag:
    
    reset_checkpoint_task = PythonOperator(
        task_id="reset_checkpoint",
        python_callable=reset_checkpoint,
        dag=dag,
        trigger_rule="all_done" 
    )
    
    # Task to process the next city
    process_next_city_task = PythonOperator(
        task_id="process_next_city",
        python_callable=process_next_city,
        dag=dag
    )
    
    # Task to clean the data
    clean_attractions_task = PythonOperator(
        task_id="clean_attractions",
        python_callable=clean_attractions_csv,
        op_kwargs={"input_path": RAW_ATTRACTIONS_PATH, "output_path": CLEAN_ATTRACTIONS_PATH},
        dag=dag
    )

    # Task to geocode the attractions
    geocode_attractions_task = PythonOperator(
        task_id="geocode_attractions",
        python_callable=add_coordinates_to_csv,
        op_kwargs={"input_path": CLEAN_ATTRACTIONS_PATH, "output_path": GEOCODED_ATTRACTIONS_PATH},
        dag=dag
    )

    # Task to upload geocoded attractions to S3
    upload_geocoded_attractions_task = PythonOperator(
        task_id="upload_geocoded_attractions",
        python_callable=upload_to_s3,
        op_kwargs={
            "file_path": GEOCODED_ATTRACTIONS_PATH,
            "bucket": s3_bucket,
            "s3_key": GEOCODED_ATTRACTIONS_S3_KEY
        },
        dag=dag
    )
    
    # Task to load geocoded attractions to Snowflake
    load_to_snowflake_task = PythonOperator(
        task_id="load_geocoded_attractions_snowflake",
        python_callable=load_from_s3_to_snowflake,
        op_kwargs={
            "s3_bucket": s3_bucket,
            "s3_key": GEOCODED_ATTRACTIONS_S3_KEY,
            "table_name": SNOWFLAKE_ATTRACTIONS_TABLE
        },
        dag=dag
    )

    # Parallel flow - either use the process_all task or the individual tasks
    process_next_city_task >> clean_attractions_task >> geocode_attractions_task >> upload_geocoded_attractions_task >> load_to_snowflake_task >> reset_checkpoint_task