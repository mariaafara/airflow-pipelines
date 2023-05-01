import pandas as pd

if __name__ == '__main__':
    # Generate a simple DataFrame
    df = pd.DataFrame({'numbers': [1, 2, 3], 'letters': ['a', 'b', 'c']})
    print(df)
    df.to_csv("dataframe.csv")
