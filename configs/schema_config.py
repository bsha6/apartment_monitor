import numpy as np

# TODO: incorporate this into scraping script
# TODO: think of better name for this
APT_DF_SCHEMA = {
    'unit_id': str,
    'price': int,
    'square_feet': int,
    'bedrooms': np.float16,
    'bathrooms': np.float16,
    'floor_plan_type': str,
    'date_available': str
}

WEBSITE_ERROR_MESSAGES = [
    '<div class="error" id="floor-plans">Unable to load apartments at this time. Please try again.</div>'
]