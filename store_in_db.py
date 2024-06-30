import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, RealDictCursor
import pandas as pd
from typing import List

from apt_webscraper import given_url_get_latest_scraped_data

# Set DB info
load_dotenv()
HOST = os.getenv("HOST")
DB_NAME = os.getenv("DB_NAME")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = os.getenv("PORT")
db_params = {
    "host": HOST,
    "database": DB_NAME,
    "user": USER,
    # "password": PASSWORD,
    "port": PORT,
}


class DBConfigManager:
    def __init__(self, db_connection):
        self.conn = db_connection

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def get_all_cols_in_table(self, table: str):
        """Given a table name, return a list of all the table's column names."""
        try:
            # Get table columns from the database
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    """,
                    (table,),
                )
                db_columns = [row[0] for row in cur.fetchall()]
                return db_columns
        except psycopg2.Error as e:
            print("Error: ", e)

    def select_cols_from_table(self, table: str, cols_list: str = "*"):
        """Given a table, select a comma separated list of columns and return rows of data."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT {cols_list} FROM {table}")
            return cur.fetchall()

    def select_all_rows_from_table(self, table: str):
        """Given a table, select and return all rows of data."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {table}")
            return cur.fetchall()

    def get_apt_info_df_given_url(
        self, url_list: List[str], cols: str = "id, div_id"
    ) -> pd.DataFrame:
        """Given a list of urls and string of comma separated SQL column names, return a df of apt specified columns and the url."""
        url_list_distinct = tuple(set(url_list))
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT {cols}, url FROM apts WHERE url IN {url_list_distinct}"
            )
            df_apt_info = pd.DataFrame(cur.fetchall())
            return df_apt_info

    def get_apt_scraping_info_given_building(
        self, building_name: str, cols: str = "id, div_id"
    ) -> pd.DataFrame:
        """Given a building name and string of comma separated SQL column names, return a df of apt data needed for scraping."""
        # TODO: ensure building_name is Capitalized.
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT {cols}, url FROM apts WHERE building_name='{building_name}'"
            )
            df_apt_scraping_info = pd.DataFrame(cur.fetchall())
            return df_apt_scraping_info

    def batch_upsert_floor_plans(self, df: pd.DataFrame):
        """Upsert (update and insert) floor plans table using a given df."""
        cursor = self.conn.cursor()
        columns = df.columns.to_list()

        # Ensure all required columns are in the DataFrame
        # TODO: Validate this can handle cases where df has floor_plan_type col (it should work lol)
        columns_check = [
            "apt_id",
            "unit_number",
            "sq_ft",
            "bedrooms",
            "bathrooms",
            "price",
            "date_available",
        ]
        missing_columns = set(columns_check) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")

        # Prepare the data
        data_to_upsert = [
            tuple(row[col_name] for col_name in columns) for _, row in df.iterrows()
        ]

        # Prepare the SQL for each column
        column_updates = [
            sql.SQL(
                "{0} = CASE WHEN EXCLUDED.{0} IS DISTINCT FROM floor_plans.{0} THEN EXCLUDED.{0} ELSE floor_plans.{0} END"
            ).format(sql.Identifier(col))
            for col in columns
            if col not in ["apt_id", "unit_number"]
        ]

        # Construct the SQL query
        insert_clause = sql.SQL("INSERT INTO floor_plans ({}) VALUES %s").format(
            sql.SQL(", ").join(map(sql.Identifier, columns))
        )

        update_clause = sql.SQL(
            "ON CONFLICT (apt_id, unit_number) DO UPDATE SET {}"
        ).format(sql.SQL(", ").join(column_updates))

        where_clause = sql.SQL("WHERE {}").format(
            sql.SQL(" OR ").join(
                sql.SQL("EXCLUDED.{0} IS DISTINCT FROM floor_plans.{0}").format(
                    sql.Identifier(col)
                )
                for col in columns
                if col not in ["apt_id", "unit_number"]
            )
        )

        upsert_query = sql.SQL("{} {} {}").format(
            insert_clause, update_clause, where_clause
        )

        execute_values(cursor, upsert_query, data_to_upsert)

        # Get all current unit numbers for this apartment
        apt_id = int(df['apt_id'].iloc[0])  # Assumes all data in df is from the same apt_id
        cursor.execute("SELECT unit_number FROM floor_plans WHERE apt_id = %s", (apt_id,))
        current_units = set(row[0] for row in cursor.fetchall())
    
        # Identify units that are no longer available
        scraped_units = set(df['unit_number'])
        units_removed = current_units - scraped_units

        # Mark units as unavailable if they're no longer in the scraped data
        if units_removed:
            update_query = sql.SQL("""
                UPDATE floor_plans 
                SET availability_status = FALSE 
                WHERE apt_id = %s AND unit_number = ANY(%s)
            """)
            cursor.execute(update_query, (apt_id, list(units_removed)))

        # TODO: Fix inconsistencies/inaccuracies with rowcount. Claude suggesting CTE.
        rows_upserted = cursor.rowcount
        self.conn.commit()
        cursor.close()
        print(
            f"Processed {len(data_to_upsert)} rows of data for apt_id {df['apt_id'].unique()}."
        )
        print(f"Actually upserted {rows_upserted} rows in the floor_plans table.")

    # TODO: modify this to work with a building_name arg
    def get_config_by_url(self, url):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM apts WHERE url = %s", (url,))
            return cur.fetchone()

    def add_config(self, url, building_name, scraper_function):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scraping_config (url, building_name, scraper_function) VALUES (%s, %s, %s)",
                (url, building_name, scraper_function),
            )
        self.conn.commit()

    def update_config(self, id, url, building_name, scraper_function):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE scraping_config SET url = %s, building_name = %s, scraper_function = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (url, building_name, scraper_function, id),
            )
        self.conn.commit()


if __name__ == "__main__":
    try:
        conn = psycopg2.connect(**db_params)
        config_manager = DBConfigManager(conn)
        # apts = config_manager.select_all_rows_from_table("apts")
        # apts_df = pd.DataFrame(apts)

        # for row in all_configs:
        #     if row['div_id']:
        #         print(given_url_get_latest_scraped_data(url=row['url'], div_id=row['div_id']))
        #     else:
        #         print(interact_scrape_and_get_df(url=row['url']))

        # TODO: store scraped data in db !
        # fp_cols = config_manager.get_all_cols_in_table("floor_plans")
        # print(fp_cols)

        # Start with building name -> scrape -> upsert [DONE]
        # TODO: convert this into a function where you can just pass the building name

        def scrape_all_buildings_in_db():
            """Scrape all of the building_names in the apts table and upsert the data into the floor_plans table."""
            buildings = config_manager.select_cols_from_table(table="apts", cols_list="building_name")
            buildings_list = [list(b.values())[0] for b in buildings]
            for b in buildings_list:
                scraping_info = config_manager.get_apt_scraping_info_given_building(b)
                apt_id = scraping_info["id"].iloc[0]
                apt_url = scraping_info["url"].iloc[0]
                apt_div_id = scraping_info["div_id"].iloc[0]

                latest_b_df = given_url_get_latest_scraped_data(
                    url=apt_url, div_id=apt_div_id
                )
                latest_b_df["apt_id"] = apt_id
                config_manager.batch_upsert_floor_plans(latest_b_df)
            print(f"Scraped and upserted {len(buildings_list)}, including: {str(*buildings_list)}")

        scrape_all_buildings_in_db()

        # WIP: Test comparing new data with historical.
        # lyric_apt_df = apts_df[apts_df['building_name'] == 'Lyric']
        # lyric_apt_id = lyric_apt_df['id'].iloc[0]
        # lyric_url = lyric_apt_df['url'].iloc[0]
        # lyric_div_id = lyric_apt_df['div_id'].iloc[0]



        # # Lookup old data from floor plans table
        # conn = psycopg2.connect(**db_params)
        # config_manager = DBConfigManager(conn)
        # floor_plans = config_manager.select_all_rows_from_table("floor_plans")
        # # TODO: filter data in SQL
        # floor_plans_df = pd.DataFrame(floor_plans)
        # lyric_fps = floor_plans_df[floor_plans_df["apt_id"] == lyric_apt_id]
        # print(lyric_fps)

    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")

    finally:
        # Close the connection when done
        if "config_manager" in locals():
            config_manager.close_connection()
