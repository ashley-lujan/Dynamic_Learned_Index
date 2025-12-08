import pandas as pd
from hasher import ExtensibleHash

def test_hasher_with_tuples():
    df = pd.DataFrame(
        {"uid": [1, 2, 3, 4, 5, 6, 7, 8], "name": ["a", "b", "c", "d", "e", "f", "g", "h"]}
    )
    def val_extractor(xi):
        return xi[1]
    hash_ds = ExtensibleHash(val_extractor, init_size=2)
    n = len(df)
    for i in range(n):
        xi = df.loc[i].values
        hash_ds.insert_item(xi)
        print(hash_ds)
        
if __name__ == "__main__":
    print('IN TESTS')
    test_hasher_with_tuples()