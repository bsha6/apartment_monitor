import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO


def get_soup(url: str) -> BeautifulSoup:
    """Given a url, make a get request and return the BeautifulSoup object of the response content."""
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

lydian_url = "https://lydianlyric.com/lydian-floor-plans-2/?type=2BR"
lydian_soup = get_soup(lydian_url)

lydian_div_id = "floor-plans"
lydian_df = parse_table_from_html(soup=lydian_soup, div_id=lydian_div_id)

print(lydian_df)

lyric_url = "https://lydianlyric.com/lyric-floor-plans/?type=2BR"
lyric_soup = get_soup(lyric_url)

lyric_df = parse_table_from_html(soup=lyric_soup, div_id=lydian_div_id)

print(lyric_df)