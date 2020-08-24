import pandas as pd


class DataLoader:
    def __init__(self):
        pass

    def load(self, file_path, columns_seletion, compression='gzip', error_bad_lines=False, rename_dict=None,
             fill_na_dict=None, concat_dict=None, include_file_path = True, source_system = "ROC", to_lower=True):
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
                filtered_df[k] = filtered_df[v].apply(lambda row: ''.join(row.values.astype(str)), axis=1)

        # Filter data by rows condition
        # TODO

        # Use Zvelo to add column
        # TODO

        # Add metadata columns
        # filtered_df["FilePath"] = file_path
        # filtered_df["SourceSystem"] = source_system

        # Convert all columns to lower
        if to_lower:
            filtered_df.columns = filtered_df.columns.str.lower()

        return filtered_df


