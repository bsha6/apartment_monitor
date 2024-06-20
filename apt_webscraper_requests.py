import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_html_content(url: str) -> str:
    """Given a url, make a get request and return the response text."""
    # Fetch the HTML content
    response = requests.get(url)
    return response.text

lydian_url = "https://lydianlyric.com/lydian-floor-plans-2/?type=2BR"
lydian_html = get_html_content(lydian_url)

soup = BeautifulSoup(html_content, 'html.parser')
div_apts = soup.find('div', id='floor-plans')

if div_apts:
    # Assuming the table is directly inside specified div id, find the table
    table = div_apts.find('table')
    
    if table:
        df = pd.read_html(str(table))[0]
    else:
        print(f"No table found inside div with id 'X' on {url}")
else:
    print(f"Div with id 'X' not found on {url}")

print(df)