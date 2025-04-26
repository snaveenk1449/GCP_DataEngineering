import pandas as pd
import sys


def parquet_to_csv(input_csv_path, output_parquet_path):
    try:
        df = pd.read_csv(input_csv_path)
        df.to_parquet(output_parquet_path)
        print(f"Successfully converted {input_csv_path} to {output_parquet_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':

    input_file = f'/Users/naveensabbarapu/Desktop/CSV_Files'
    output_file = f'/Users/naveensabbarapu/Desktop/Parquet_Files'
    parquet_to_csv(input_file, output_file)
