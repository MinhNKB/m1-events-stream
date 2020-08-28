import pandas as pd
import functools
import logging

class DataLoader:
    def __init__(self):
        pass

    def load(self, byte_io, columns_seletion, compression='gzip', error_bad_lines=False,
             fill_na_dict=None, concat_dict=None, rename_dict=None, to_lower=True):
        # Load raw data
        raw_df = pd.read_csv(byte_io, compression=compression, error_bad_lines=error_bad_lines, dtype=str)

        # Filter selected columns
        filtered_df = raw_df.filter(columns_seletion)

        # Fill NA by specific values
        if fill_na_dict is not None:
            for k,v in fill_na_dict.items():
                filtered_df[k].fillna(v, inplace=True)

        # Concatenate multiple columns to a new one
        if concat_dict is not None:
            for k, v in concat_dict.items():
                filtered_df[k] = pd.Series(map("".join,filtered_df[v].values.tolist()),index=filtered_df.index)

        # Filter data by rows condition
        filtered_df = filtered_df[(filtered_df["AppNumber"] != 10) | (filtered_df["DownloadVolume"] > 0)]


        # Use Zvelo to add column
        # TODO
        # filtered_df["category0"] = ""
        # filtered_df["category1"] = ""
        # filtered_df["category2"] = ""

        # Rename columns
        if rename_dict is not None:
            filtered_df.rename(columns=rename_dict, inplace=True)

        # Convert all columns to lower
        if to_lower:
            filtered_df.columns = filtered_df.columns.str.lower()

        return filtered_df


