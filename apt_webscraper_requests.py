import requests
from bs4 import BeautifulSoup
import pandas as pd


# Fetch the HTML content
url = "https://lydianlyric.com/lydian-floor-plans-2/?type=2BR"
response = requests.get(url)
html_content = response.text

# Parse HTML with BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

# Find the specific div with id 'X'
div_apts = soup.find('div', id='floor-plans')

if div_apts:
    # Assuming the table is directly inside div_X, find the table
    table = div_X.find('table')
    
    if table:
        df = pd.read_html(str(table))[0]
        # # Process table rows and cells to extract data
        # for row in table.find_all('tr'):
        #     # Process each cell in the row
        #     cells = row.find_all('td')
        #     if cells:  # Assuming it's a data row (not header/footer)
        #         # Example: Print the content of each cell
        #         for cell in cells:
        #             print(cell.text.strip())  # or do something with cell content
    else:
        print(f"No table found inside div with id 'X' on {url}")
else:
    print(f"Div with id 'X' not found on {url}")

print(df)