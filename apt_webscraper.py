import time
import sys
import os
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import numpy as np
from io import StringIO
from dotenv import load_dotenv

from configs.schema_config import WEBSITE_ERROR_MESSAGES

load_dotenv()
UTILS_PATH = os.getenv("UTILS_FOLDER_PATH")
sys.path.insert(0, UTILS_PATH)

from utils.string_utils import extract_digits_from_text, contains_digits  # noqa: E402
from utils.pd_df_ops import cast_df_and_rename_cols  # noqa: E402


def get_soup(url: str) -> BeautifulSoup:
    """
    Given a url, make a get request and return the BeautifulSoup object of the response content.
    This function is for websites that don't require any clicking to get all of the apartment data.
    """
    # Fetch the HTML content
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    return soup


def parse_table_from_html(soup: BeautifulSoup, div_id: str) -> pd.DataFrame:
    """
    Given the html of a webpage and a div id, extract a table as a pandas df.
    Note: this assumes a table is nested inside a div id.
    """
    div_apts = soup.find("div", id=div_id)
    if div_apts is None:
        raise ValueError(f"Div with id {div_id} not found")
    table = div_apts.find("table")
    # TODO: improve website error handling/make this more robust
    if table is None:
        if div_apts in WEBSITE_ERROR_MESSAGES:
            print("Not finding a table with div_id {div_id}. This is what bs4 is finding with specified div_id:", div_apts)
        raise ValueError(f"No table found inside div with id {div_id}")
    df = pd.read_html(StringIO(str(table)))[0]
    return df


def clean_df_by_url(df: pd.DataFrame, url: str) -> pd.DataFrame:
    """
    Cleans the Pandas df based on the provided url.
    Handles bespoke data cleaning and processing.
    """
    if "lydianlyric" in url:
        df["bedrooms"], df["bathrooms"] = zip(*df["BED/BATH"].str.split(" / ", n=1))
        df = df.drop(columns=["DETAILS", "APPLY NOW", "BED/BATH", "Building"])
        cols_to_extract_digits = ["RENT *", "bedrooms", "bathrooms", "SQ FT **"]
        df[cols_to_extract_digits] = df[cols_to_extract_digits].map(
            extract_digits_from_text
        )
    else:
        raise ValueError(f"URL '{url}' is not recognized.")
    return df


def scrape_parse_and_read_html(url: str, div_id: str) -> pd.DataFrame:
    """
    Scrapes, parses, and reads HTML content from a given URL and extracts data into a Pandas df.
    Through the url, set a schema and a dictionary of columns to be renamed. This needs to be done for each new url.
    """
    if "lydianlyric" in url:
        # TODO: rework this to rename columns and then set schema at end? Important to make sure unit number stays as a string.
        schema = {
            "UNIT NUMBER": str,
            "RENT *": int,
            "SQ FT **": int,
            "bedrooms": np.float16,
            "bathrooms": np.float16,
        }
        col_rename_mapping = {
            "RENT *": "price",
        }
    else:
        raise ValueError(f"URL '{url}' is not recognized.")
    soup = get_soup(url)
    df = parse_table_from_html(soup, div_id)
    df_cleaned = clean_df_by_url(df, url)
    df_cleaned = cast_df_and_rename_cols(df_cleaned, schema, col_rename_mapping)
    return df_cleaned


def interact_and_scrape_website(url: str) -> BeautifulSoup:
    """
    Depending on what website, take action to show all apt data, and return a BeautifulSoup object.
    """
    driver_options = webdriver.chrome.options.Options()
    driver_options.add_argument("--headless=new")
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=driver_options)

    try:
        driver.get(url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        if "450k" in url:
            available_apts_header = soup.find("div", class_="available_apartmnt").get_text(strip=True)

            if available_apts_header is None:
                raise ValueError("Could not find the 'available_apartmnt' div")
        
            available_apts_str = available_apts_header.strip().split(sep=" ")[0]
            try:
                available_apts = int(available_apts_str)
            except ValueError as e:
                raise ValueError(f"Failed to convert '{available_apts}' to an integer: {str(e)}") from e

            if available_apts == 0:
                print(f"No available apartments found at {url}")
            elif available_apts > 6:
                time.sleep(1)
                # Scroll to the bottom to force cookie pop up to minimize.
                driver.execute_script("window.scrollBy(0, 1800);")
                # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                try:
                    # Finding by element works more reliably than by xpath.
                    load_more_button = driver.find_element_by_id("btn_loadmore")
                    load_more_button.click()
                    # Need to wait for additional data to load after clicking button.
                    time.sleep(3)
                except Exception as e:
                    print(f"Could not click load more button. Error: {e}")

            elif available_apts <= 6:
                print(f"Don't need to load more since only {available_apts} available apartments at {url}")
            else:
                raise ValueError(f"URL '{url}' is not recognized.")
            
        return soup

    except Exception as e:
        print(f"Error during scraping url {url}. Error: {e}")
        print(f"Page source: {driver.page_source[:500]}...") 

    finally:
        driver.quit()


def parse_html_to_df(url: str, soup: BeautifulSoup) -> pd.DataFrame:
    """Given BeautifulSoup object, parse by url and return a cleaned df of apartment information."""
    if "450k" in url:
        div_class_450k = "fp_lists"
        soup_floor_plans = soup.find("div", class_=div_class_450k)
        # Extract apartment data from html by iterating through each floor plan (fp) block.
        # FPs after "loading more" don't have <p> classes to reference like other FPs.
        apartments = []
        for fp_block in soup_floor_plans.find_all("div", class_="fp_block"):
            unit_number = fp_block.find("p", class_="fp_no").text.strip()
            unit_number_extracted = extract_digits_from_text(unit_number)
            description = fp_block.find_all("p")
            bed_and_bath = description[1].text.strip().split("+")
            bedrooms, bathrooms = [
                extract_digits_from_text(b) if contains_digits(b) else 0
                for b in bed_and_bath
            ]
            floor_plan_type = description[2].text.strip()
            sq_ft = description[3].text.strip().split(" ")[0]
            price_string = description[4].text.strip().replace(",", "")
            price_extracted = extract_digits_from_text(price_string)
            date_available = description[5].text.strip().replace("AVAILABLE ", "")
            apartments.append(
                (
                    unit_number_extracted,
                    bedrooms,
                    bathrooms,
                    sq_ft,
                    floor_plan_type,
                    price_extracted,
                    date_available,
                )
            )

        # Convert to dataframe, set types
        # All of these columns should always have the same types. Can have global dict with types?
        df_450k = pd.DataFrame(
            apartments,
            columns=[
                "unit_number",
                "bedrooms",
                "bathrooms",
                "sq_ft",
                "floor_plan_type",
                "price",
                "date_available",
            ],
        )
        df_450k = df_450k.astype(
            {
                "bedrooms": np.float16,
                "bathrooms": np.float16,
                "sq_ft": int,
                "price": int,
            }
        )
        return df_450k
    else:
        raise ValueError(f"URL '{url}' is not recognized.")


def interact_scrape_and_get_df(url: str) -> pd.DataFrame:
    # TODO: will modify this to write to SQLite DB
    soup = interact_and_scrape_website(url)
    df = parse_html_to_df(url, soup)
    return df


def given_url_get_latest_scraped_data(url: str, div_id: str = None) -> pd.DataFrame:
    """Dispatcher function that takes a url, determines how it needs to be scraped, calls the necessary functions, and returns a cleaned df."""
    # TODO: make this function more robust for other cases/websites
    # TODO: Add unit test/checks/raise errors
    if div_id:
        df = scrape_parse_and_read_html(url, div_id)
        return df
    elif "450k" in url:
        df = interact_scrape_and_get_df(url)
        return df
    elif not div_id:
        raise ValueError(
            f"Don't recognize url: {url}. Did you mean to specify a div_id?"
        )
    else:
        raise ValueError(f"Don't recognize url: {url}.")


if __name__ == "__main__":
    url_450k = "https://www.450k.com/floor-plans/apartments?two-bed=1"
    df_450k = interact_scrape_and_get_df(url_450k)
    print(df_450k)

    lydian_url = "https://lydianlyric.com/lydian-floor-plans-2/?type=2BR"
    lydian_div_id = "floor-plans"
    # lydian_df = scrape_parse_and_read_html(url=lydian_url, div_id=lydian_div_id)
    # print(lydian_df)

    lyric_url = "https://lydianlyric.com/lyric-floor-plans/?type=2BR"
    # lyric_df = scrape_parse_and_read_html(url=lyric_url, div_id=lydian_div_id)
    # print(lyric_df)

    # print(given_url_get_latest_scraped_data(lydian_url, div_id=lydian_div_id))


