import pandas as pd
from argparse import ArgumentParser
import os

env_var = os.getenv("env_var")

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(
        "--data_folder",
        type=str,
        required=True,
        dest="data_folder",
        help="Directory Path to where the df will be stored.",
    )
    args = parser.parse_args()

    # Generate a simple DataFrame
    df = pd.DataFrame({'numbers': [1, 2, 3], 'letters': ['a', 'b', 'c']})
    print(df)
    df.to_csv(os.path.join(args.data_folder, "dataframe.csv"))

    print(f"ENV VAR = {env_var}")
