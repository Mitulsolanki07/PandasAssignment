import pandas as pd
import numpy as np
import requests
from typing import List

class CleanDataFrame:
    
    @staticmethod
    def MSCleanStrTypeColumn(series: pd.Series) -> pd.Series:
        """
        Cleans a series of string type by performing the following operations:
        - Removes leading and trailing white spaces
        - Removes newline characters
        - Capitalizes all words
        - Replaces NaN values with an empty string

        Parameters:
        series (pd.Series): The input pandas Series to clean.

        Returns:
        pd.Series: The cleaned pandas Series.
        """
        cleaned_series = series.astype(str)  # Ensure the series is of string type
        cleaned_series = cleaned_series.str.strip()  # Remove leading and trailing white spaces
        cleaned_series = cleaned_series.str.replace('\n', '')  # Remove newline characters
        cleaned_series = cleaned_series.str.title()  
        cleaned_series = cleaned_series.replace('nan', '', regex=True)  # Replace NaN values with empty string
        return cleaned_series

    @staticmethod
    def MSCleanFloatTypeColumn(series: pd.Series) -> pd.Series:
        """
        Cleans a series of float type by performing the following operations:
        - Removes null values and replaces them with an empty string
        - Replaces empty strings with NaN
        - Converts to float and rounds to 1 decimal place
        - Replaces commas in strings to handle human-readable formats

        Parameters:
        series (pd.Series): The input pandas Series to clean.

        Returns:
        pd.Series: The cleaned pandas Series.
        """
        # Replace empty strings with NaN
        series = series.replace('', np.nan)
        
        # Convert series to string to handle string operations
        series = series.astype(str)
        
        # Remove commas from the strings
        series = series.str.replace(',', '')
        
        # Convert to float and round to 1 decimal place
        series = pd.to_numeric(series, errors='coerce').round(1)
        
        # Replace NaN values with empty string
        series = series.fillna('')
        
        return series



class DataFetcher:
    
    @staticmethod
    def MSFetchJsonData(url: str) -> dict:
        """Fetch JSON data from the given URL."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch data: {e}")
        except ValueError as e:
            raise Exception(f"Invalid JSON data: {e}")
    
    @staticmethod
    def MSExtractColHeaders(data: dict) -> List[str]:
        """Extract column headers from the JSON data."""
        if "fields" not in data:
            raise ValueError("Missing 'fields' key in JSON data")
        
        ls_column_headers = []
        ls_field_data = data["fields"]
        
        for dict_column_data in ls_field_data:
            if isinstance(dict_column_data, dict) and isinstance(dict_column_data.get("id"), str):
                ls_column_headers.append(dict_column_data["id"])
        
        if not ls_column_headers:
            raise ValueError("No valid column headers found in 'fields'")
        
        return ls_column_headers
    
    @staticmethod
    def MSCreateDF(data: dict, column_headers: List[str]) -> pd.DataFrame:
        """Create DataFrame from JSON data using the extracted column headers."""
        if "records" not in data:
            raise ValueError("Missing 'records' key in JSON data")
        
        try:
            df = pd.DataFrame(data["records"])
            df.columns = column_headers
            return df
        except Exception as e:
            raise Exception(f"Failed to create DataFrame: {e}")
    
    @staticmethod
    def MSCapitalizeCols(df: pd.DataFrame) -> pd.DataFrame:
        """Capitalize the column headers of the DataFrame."""
        df.columns = df.columns.str.title()
        return df