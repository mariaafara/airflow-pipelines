import pandas as pd
from argparse import ArgumentParser

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(
        "--df_file_path",
        type=str,
        required=True,
        dest="df_file_path",
        help="Path to the df csv file.",
    )
    args = parser.parse_args()

    df_path = args.df_file_path
    df = pd.read_csv(df_path)
    print(df)
