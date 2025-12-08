import parsers
import pandas as pd

if __name__ == "__main__":
    df = pd.DataFrame({"name": [1, 2, 3], "dog": ["a", "b", "c"]})
    print(df)
    print(df.columns.tolist())
    print(df.loc[0].values[1])