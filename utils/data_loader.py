import pandas as pd
import functools
import logging

class DataLoader:
    def __init__(self):
        pass

    def load(self, file_path, columns_seletion, compression='gzip', error_bad_lines=False, rename_dict=None,
             fill_na_dict=None, concat_dict=None, to_lower=True):
        # Load raw data
        raw_df = pd.read_csv(file_path, compression=compression, error_bad_lines=error_bad_lines)

        # Filter selected columns
        filtered_df = raw_df.filter(columns_seletion)

        # Rename columns
        if rename_dict is not None:
            filtered_df.rename(columns=rename_dict, inplace=True)

        # Fill NA by specific values
        if fill_na_dict is not None:
            for k,v in fill_na_dict.items():
                filtered_df[k].fillna(v, inplace=True)

        # Concatenate multiple columns to a new one
        if concat_dict is not None:
            for k, v in concat_dict.items():
                filtered_df[k] = pd.Series(map("".join,filtered_df[v].values.tolist()),index=filtered_df.index)

        # Filter data by rows condition
        # TODO

        # Use Zvelo to add column
        # TODO

        # Convert all columns to lower
        if to_lower:
            filtered_df.columns = filtered_df.columns.str.lower()

        return filtered_df


