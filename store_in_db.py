import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd

from apt_webscraper import scrape_parse_and_read_html, interact_scrape_and_get_df

# Set DB info
load_dotenv()
HOST = os.getenv('HOST')
DB_NAME = os.getenv('DB_NAME')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
PORT = os.getenv('PORT')
db_params = {
    "host": HOST,
    "database": DB_NAME,
    "user": USER,
    # "password": PASSWORD,
    "port": PORT
}

class DBConfigManager:
    def __init__(self, db_connection):
        self.conn = db_connection

    def get_all_configs(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM apts")
            return cur.fetchall()

    def get_config_by_url(self, url):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM apts WHERE url = %s", (url,))
            return cur.fetchone()

    def add_config(self, url, building_name, scraper_function):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scraping_config (url, building_name, scraper_function) VALUES (%s, %s, %s)",
                (url, building_name, scraper_function)
            )
        self.conn.commit()

    def update_config(self, id, url, building_name, scraper_function):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE scraping_config SET url = %s, building_name = %s, scraper_function = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (url, building_name, scraper_function, id)
            )
        self.conn.commit()


if __name__ == "__main__":
    conn = psycopg2.connect(**db_params)
    config_manager = DBConfigManager(conn)
    all_configs = config_manager.get_all_configs()
    all_configs_df = pd.DataFrame(all_configs)
    print(all_configs_df)

    for row in all_configs:
        if row['div_id']:
            print(scrape_parse_and_read_html(url=row['url'], div_id=row['div_id']))
        else:
            print(interact_scrape_and_get_df(url=row['url']))
