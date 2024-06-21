import time
from selenium import webdriver
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import numpy as np
import re

from utils.string_utils import extract_digits_from_text


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
            time.sleep(2)
            # Scroll to the bottom to force cookie pop up to minimize.
            driver.execute_script(f"window.scrollBy(0, 1800);")
            # driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            try:
                # Finding by element works more reliably than by xpath.
                load_more_button = driver.find_element_by_id('btn_loadmore')
                load_more_button.click()
                # Need to wait for additional data to load after clicking button.
                time.sleep(4)
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
    # url = "https://lydianlyric.com/lydian-floor-plans-2/?type=2BR"
    url_450k = "https://www.450k.com/floor-plans/apartments?two-bed"
    df_450k = scrape_parse_and_get_df(url_450k)
    print(df_450k)
    print(df_450k.info())