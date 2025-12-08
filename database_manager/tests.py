import pandas as pd
from hasher import ExtensibleHash
import numpy as np

def test_hasher_with_tuples(log=False):
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
        if log:
            print(hash_ds)
    print("PASSED TEST HASHER")
    
def test_hasher_with_tuples_extraction(log=False):
    data = {"uid": [1, 2, 3, 4, 5, 6, 7, 8, 9], "name": ["a", "b", "c", "d", "e", "f", "g", "h", "j"]}
    df = pd.DataFrame(data)
    def val_extractor(xi):
        return xi[1]
    hash_ds = ExtensibleHash(val_extractor, init_size=2)
    n = len(df)
    for i in range(n):
        xi = df.loc[i].values
        hash_ds.insert_item(xi)
    
    if (log):
        print(hash_ds)
            
    for i, name in enumerate(data["name"]):
        i += 1
        result = hash_ds.get(name)
        if (log):
            print('for name = {}, result is: {}'.format(name, result))
        assert result is not None
        assert result[0].item() == i
        
if __name__ == "__main__":
    print('IN TESTS')
    test_hasher_with_tuples()
    test_hasher_with_tuples_extraction()