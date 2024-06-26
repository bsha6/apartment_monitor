import numpy as np

# TODO: incorporate this into scraping script
APT_DF_SCHEMA = {
    'unit_id': str,
    'price': int,
    'square_feet': int,
    'bedrooms': np.float16,
    'bathrooms': np.float16,
    'floor_plan_type': str,
    'date_available': str
}