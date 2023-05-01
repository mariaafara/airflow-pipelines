import sys
import pandas as pd

if __name__ == '__main__':
    df_path = sys.argv[1]
    df = pd.read_csv(df_path)
    print(df)
