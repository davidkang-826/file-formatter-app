import uuid

import pandas as pd
import streamlit as st
import os

class FileFormatterApp:
    def __init__(self):
        pass


    def csv_to_dataframe(self, file_path):
        """
        Converts a CSV file to a pandas DataFrame and returns a map
        with a unique ID as key and DataFrame as value.

        Parameters:
        - file_path: str, the path to the CSV file

        Returns:
        - dict: {UUID string: pd.DataFrame}
        """
        try:
            df = pd.read_csv(file_path)
            unique_id = str(uuid.uuid4())
            # unique_id = os.path.basename(file_path)  # "data1.csv" # if I want it based on file path name
            return {unique_id: df}
        except FileNotFoundError:
            print(f"Error: File not found at '{file_path}'")
        except pd.errors.EmptyDataError:
            print("Error: The file is empty")
        except pd.errors.ParserError:
            print("Error: The file could not be parsed as CSV")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def combine_dataframes_from_map(self, df_map):
        """
        Combines all DataFrames from the provided map into a single DataFrame,
        removing duplicate rows (like SQL UNION).

        Parameters:
        - df_map (dict): {uuid: pd.DataFrame}

        Returns:
        - pd.DataFrame: combined and de-duplicated DataFrame
        """
        try:
            if not df_map:
                print("Warning: No DataFrames to combine.")
                return pd.DataFrame()  # Return empty DataFrame if map is empty

            all_dfs = list(df_map.values())
            combined = pd.concat(all_dfs, ignore_index=True)
            deduplicated = combined.drop_duplicates()
            return deduplicated

        except Exception as e:
            print(f"Error while combining DataFrames: {e}")
            return pd.DataFrame()  # Fallback to empty DataFrame on error
