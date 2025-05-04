import os
import psycopg2
import json # Import json module
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from google.cloud import storage
from dotenv import load_dotenv
from datetime import datetime
import logging
import sys
from urllib.parse import quote_plus # To properly encode filename for URL
import time

# --- Configuration ---
# Load environment variables from .env.scraper file in the same directory
dotenv_path = os.path.join(os.path.dirname(__file__), '.env.scraper')
load_dotenv(dotenv_path=dotenv_path)

# Database Configuration
DB_URL = os.getenv("DATABASE_URL")

# GCS Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_FOLDER_NAME = os.getenv("GCS_FOLDER_NAME", "scraped_jobs") # Default to 'scraped_jobs' if not set
# GOOGLE_APPLICATION_CREDENTIALS should be set in the environment where the script runs

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Playwright Configuration
# Consider adding options like headless=True for server environments
PLAYWRIGHT_TIMEOUT = 60000 # 60 seconds for page load and operations

# --- Helper Functions ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DB_URL)
        logging.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to database: {e}")
        sys.exit(1)

def get_gcs_client():
    """Initializes and returns a Google Cloud Storage client."""
    try:
        storage_client = storage.Client()
        logging.info("Successfully initialized GCS client.")
        return storage_client
    except Exception as e:
        logging.error(f"Error initializing GCS client: {e}")
        logging.error("Ensure GOOGLE_APPLICATION_CREDENTIALS environment variable is set correctly.")
        sys.exit(1)

def fetch_jobs_to_scrape(conn):
    """Fetches jobs with 'PENDING' or 'FAILED' scraping status."""
    jobs = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, url
                FROM "Job"
                WHERE "scrapingStatus" IN ('PENDING', 'FAILED')
                ORDER BY "ingestedAt" ASC;
            """)
            jobs = cur.fetchall()
            logging.info(f"Found {len(jobs)} jobs to scrape.")
    except psycopg2.Error as e:
        logging.error(f"Error fetching jobs: {e}")
    return jobs

def scrape_website_with_playwright(p, url):
    """Scrapes the website using Playwright, gets HTML and screenshot."""
    browser = None
    page_content = None
    screenshot_bytes = None
    try:
        browser = p.chromium.launch(headless=True) # Run headless for servers
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        page = context.new_page()
        page.goto(url, timeout=PLAYWRIGHT_TIMEOUT, wait_until='domcontentloaded') # Wait for DOM load initially
        
        # Optional: Add a small delay or wait for a specific element if content loads dynamically
        time.sleep(5) # Wait 5 seconds for dynamic content - adjust as needed
        # Example: page.wait_for_selector('div#job-description', timeout=PLAYWRIGHT_TIMEOUT)

        page_content = page.content() # Get full HTML after potential JS execution
        screenshot_bytes = page.screenshot(full_page=True) # Take full page screenshot

        logging.info(f"Successfully scraped URL: {url}")
        
    except PlaywrightTimeoutError:
        logging.error(f"Timeout error scraping URL {url}")
    except PlaywrightError as e:
        logging.error(f"Playwright error scraping URL {url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error scraping URL {url}: {e}")
    finally:
        if browser:
            browser.close()
            
    return page_content, screenshot_bytes


def upload_scrape_data_to_gcs(storage_client, html_content, screenshot_bytes, job_id):
    """Uploads scraped HTML (as JSON) and screenshot to GCS, returns JSON file URL."""
    if not GCS_BUCKET_NAME:
        logging.error("GCS_BUCKET_NAME environment variable is not set.")
        return None

    json_url = None
    screenshot_url = None # Keep track of screenshot URL for logging

    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        safe_job_id = quote_plus(job_id)
        base_blob_path = f"{GCS_FOLDER_NAME}/{safe_job_id}" if GCS_FOLDER_NAME else safe_job_id

        # 1. Prepare and Upload JSON content
        json_blob_name = f"{base_blob_path}.json"
        json_blob = bucket.blob(json_blob_name)
        scrape_data = {"html": html_content}
        json_content = json.dumps(scrape_data, indent=2) # Pretty print JSON
        json_blob.upload_from_string(json_content, content_type='application/json; charset=utf-8')
        json_url = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{json_blob.name}"
        logging.info(f"Successfully uploaded JSON for job {job_id} to {json_url}")

        # 2. Upload Screenshot
        if screenshot_bytes:
            screenshot_blob_name = f"{base_blob_path}.png"
            screenshot_blob = bucket.blob(screenshot_blob_name)
            screenshot_blob.upload_from_string(screenshot_bytes, content_type='image/png')
            screenshot_url = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{screenshot_blob.name}"
            logging.info(f"Successfully uploaded screenshot for job {job_id} to {screenshot_url}")
        else:
             logging.warning(f"No screenshot bytes provided for job {job_id}. Skipping screenshot upload.")


    except Exception as e:
        logging.error(f"Error uploading to GCS for job {job_id}: {e}")
        return None # Return None if any part of the upload fails

    # Return the URL of the JSON file as requested
    return json_url


def update_job_status(conn, job_id, json_gcs_url):
    """Updates the job status to 'COMPLETED' and stores the JSON GCS URL."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE "Job"
                SET "scrapingStatus" = 'COMPLETED',
                    "scrapedContentUrl" = %s, -- Store JSON URL here
                    "scrapedAt" = %s,
                    "lastScrapingAttempt" = %s,
                    "scrapingError" = NULL
                WHERE id = %s;
            """, (json_gcs_url, datetime.utcnow(), datetime.utcnow(), job_id))
        conn.commit()
        logging.info(f"Successfully updated job {job_id} status to COMPLETED with JSON URL.")
    except psycopg2.Error as e:
        logging.error(f"Error updating job {job_id}: {e}")
        conn.rollback()

# --- Main Execution ---

def main():
    logging.info("Starting scraping process with Playwright...")

    db_conn = get_db_connection()
    gcs_client = get_gcs_client()

    if not db_conn or not gcs_client:
        logging.error("Exiting due to connection errors.")
        sys.exit(1)

    jobs_to_process = fetch_jobs_to_scrape(db_conn)

    success_count = 0
    failure_count = 0

    # Initialize Playwright outside the loop
    with sync_playwright() as p:
        for job_id, job_url in jobs_to_process:
            logging.info(f"Processing job ID: {job_id}, URL: {job_url}")

            html_content, screenshot_bytes = scrape_website_with_playwright(p, job_url)

            if html_content and screenshot_bytes: # Require both for success now
                json_gcs_url = upload_scrape_data_to_gcs(gcs_client, html_content, screenshot_bytes, job_id)
                if json_gcs_url:
                    update_job_status(db_conn, job_id, json_gcs_url)
                    success_count += 1
                else:
                    # GCS upload failed
                    logging.error(f"GCS upload failed for job {job_id}. Status remains unchanged.")
                    failure_count += 1
            else:
                # Scraping failed (didn't get content or screenshot)
                logging.error(f"Scraping failed for job {job_id}. Status remains unchanged.")
                failure_count += 1

    logging.info("Scraping process finished.")
    logging.info(f"Successfully processed: {success_count}")
    logging.info(f"Failed attempts (Scraping or GCS Upload): {failure_count}")

    # Close the database connection
    if db_conn:
        db_conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    # Ensure browsers are installed before running main
    # It's often better to run `playwright install` manually once after pip install
    # but this check can be helpful.
    try:
        # This is a simple check; a more robust check might involve checking specific browser paths
        with sync_playwright() as p:
             p.chromium.launch(headless=True).close() 
        logging.info("Playwright browser check successful.")
    except PlaywrightError:
        logging.error("Playwright browsers not installed. Please run 'playwright install' in your terminal.")
        sys.exit(1)
        
    main()
