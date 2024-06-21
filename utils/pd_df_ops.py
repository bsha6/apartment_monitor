import pandas as pd
from typing import Dict

from string_utils import snakecase_text

# For functions that operate on a pandas dataframe


def cast_df_and_rename_cols(df: pd.DataFrame, schema: Dict, renamed_col_mapping: Dict) -> pd.DataFrame:
    """Given a pandas df cast as types mentioned in the schema dictionary argument."""
    df = df.astype(schema)
    df = df.rename(columns=renamed_col_mapping)
    df.rename(columns=lambda x: snakecase_text(x.strip()), inplace=True)
    return df

