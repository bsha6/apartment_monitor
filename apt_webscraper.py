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

load_dotenv()
UTILS_PATH = os.getenv('UTILS_FOLDER_PATH')
sys.path.insert(0, UTILS_PATH)

from utils.string_utils import extract_digits_from_text
from utils.pd_df_ops import cast_df_and_rename_cols


def get_soup(url: str) -> BeautifulSoup:
    """
    Given a url, make a get request and return the BeautifulSoup object of the response content.
    This function is for websites that don't require any clicking to get all of the apartment data.
    """
    # Fetch the HTML content
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup


def parse_table_from_html(soup: BeautifulSoup, div_id: str) -> pd.DataFrame:
    """
    Given the html of a webpage and a div id, extract a table as a pandas df.
    Note: this assumes a table is nested inside a div id.
    """
    div_apts = soup.find('div', id=div_id)
    if div_apts is None:
        raise ValueError(f"Div with id {div_id} not found")
    table = div_apts.find('table')
    if table is None:
        raise ValueError(f"No table found inside div with id {div_id}")
    df = pd.read_html(StringIO(str(table)))[0]
    return df


def clean_df_by_url(df: pd.DataFrame, url: str) -> pd.DataFrame:
    if 'lydianlyric' in url:
        df["bedrooms"], df["bathrooms"] = zip(*df["BED/BATH"].str.split(" / ", n=1))
        df = df.drop(columns=["DETAILS", "APPLY NOW", "BED/BATH"])
        cols_to_extract_digits = ["RENT *", "bedrooms", "bathrooms", "SQ FT **"]
        df[cols_to_extract_digits] = df[cols_to_extract_digits].map(extract_digits_from_text)
    else:
        raise ValueError(f"URL '{url}' is not recognized.")
    return df


def scrape_parse_and_read_html(url: str, div_id: str) -> pd.DataFrame:
    if "lydianlyric" in url:
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
    driver_options.add_argument('--headless=new')
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=driver_options)

    try:
        driver.get(url)

        if "450k" in url:
            time.sleep(1)
            # Scroll to the bottom to force cookie pop up to minimize.
            driver.execute_script(f"window.scrollBy(0, 1800);")
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            try:
                # Finding by element works more reliably than by xpath.
                load_more_button = driver.find_element_by_id('btn_loadmore')
                load_more_button.click()
                # Need to wait for additional data to load after clicking button.
                time.sleep(3)
            except Exception as e:
                print(f"Could not click load more button. Error: {e}")
            
        else:
            raise ValueError(f"URL '{url}' is not recognized.")

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        return soup

    except Exception as e:
        print(f"Error during scraping url {url}. Error: {e}")

    finally:
        driver.quit()


def parse_html_to_df(url: str, soup: BeautifulSoup) -> pd.DataFrame:
    """Given BeautifulSoup object, parse by url and return a cleaned df of apartment information."""
    if "450k" in url:
        div_class_450k = "fp_lists"
        soup_floor_plans = soup.find('div', class_=div_class_450k)
        # Extract apartment data from html by iterating through each floor plan (fp) block.
        # FPs after "loading more" don't have <p> classes to reference like other FPs.
        apartments = []
        for fp_block in soup_floor_plans.find_all('div', class_='fp_block'):
            unit_number = fp_block.find('p', class_='fp_no').text.strip()
            unit_number_extracted = extract_digits_from_text(unit_number)
            description = fp_block.find_all('p')
            bed_and_bath = description[1].text.strip().split("+")
            bedrooms, bathrooms = [extract_digits_from_text(b) for b in bed_and_bath]
            floor_plan_type = description[2].text.strip()
            sq_ft = description[3].text.strip().split(" ")[0]
            price_string = description[4].text.strip().replace(',', '')
            price_extracted = extract_digits_from_text(price_string)
            date_availabile = description[5].text.strip().replace('AVAILABLE ', '')
            apartments.append((unit_number_extracted, bedrooms, bathrooms, sq_ft, floor_plan_type, price_extracted, date_availabile))

        # Convert to dataframe, set types
        df_450k = pd.DataFrame(apartments, columns=['unit_number', 'bedrooms', 'bathrooms', 'sq_ft', 'floor_plan_type', 'price', 'date_availabile'])
        df_450k = df_450k.astype(
            {
                "bedrooms": np.float16,
                "bathrooms": np.float16,
                "sq_ft": int,
                "price": int
            }
        )
        return df_450k
    else:
        raise ValueError(f"URL '{url}' is not recognized.")


def scrape_parse_and_get_df(url: str) -> pd.DataFrame:
    # TODO: will modify this to write to SQLite DB
    soup = interact_and_scrape_website(url)
    df = parse_html_to_df(url, soup)
    return df

if __name__ == "__main__":
    url_450k = "https://www.450k.com/floor-plans/apartments?two-bed"
    df_450k = scrape_parse_and_get_df(url_450k)
    print(df_450k)

    lydian_url = "https://lydianlyric.com/lydian-floor-plans-2/?type=2BR"
    lydian_div_id = "floor-plans"
    lydian_df = scrape_parse_and_read_html(url=lydian_url, div_id=lydian_div_id)
    print(lydian_df)

    lyric_url = "https://lydianlyric.com/lyric-floor-plans/?type=2BR"
    lyric_df = scrape_parse_and_read_html(url=lyric_url, div_id=lydian_div_id)

    print(lyric_df)