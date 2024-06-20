import requests
from bs4 import BeautifulSoup
import pandas as pd


def get_html_content(url: str) -> str:
    """Given a url, make a get request and return the response text."""
    # Fetch the HTML content
    response = requests.get(url)
    return response.text


def parse_table_from_html(html_content: str, div_id: str) -> pd.DataFrame:
    """
    Given the html of a webpage and a div id, extract a table as a pandas df.
    Note: this assumes a table is nested inside a div id.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    div_apts = soup.find('div', id=div_id)
    if div_apts is None:
        raise ValueError(f"Div with id {div_id} not found")
    table = div_apts.find('table')
    if table is None:
        raise ValueError(f"No table found inside div with id {div_id}")
    df = pd.read_html(str(table))[0]
    return df

lydian_url = "https://lydianlyric.com/lydian-floor-plans-2/?type=2BR"
lydian_html = get_html_content(lydian_url)

lydian_div_id = "floor-plans"
lydian_df = parse_table_from_html(html_content=lydian_html, div_id=lydian_div_id)

print(lydian_df)