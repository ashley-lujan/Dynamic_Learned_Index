import pandas as pd
from table import DBTable
import math

def split(dir_name, filename, initial_size):
    df = pd.read_csv(dir_name + filename + '.csv')
    df = df.sample(frac=1) #randomize rows
    n = len(df)
    n1 = math.ceil(len(df) * initial_size)
    n2 = len(df) - n1
    initial_df = df.iloc[0:n1]
    insert_df = df.iloc[n1:n] 
    
    #save files
    initial = dir_name + filename + "_initial" + ".csv"
    initial_df.to_csv(initial, index=False)
    insert = dir_name + filename + "_insert" + ".csv"
    insert_df.to_csv(insert, index=False)
    return (initial, initial_df), (insert, insert_df)
    


def test_accuracy(initial_df, sort_key, table):
    accuracy = 0
    n = len(initial_df)
    print('n = ', n)
    for i in range(n):
        row = initial_df.iloc[i]
        result = table.select(row[sort_key])
        if result is not None:
            accuracy += 1
    return accuracy

if __name__ == "__main__":
    dir_name = 'data/original_csv/'
    filename = 'sailors'
    
    data = {"uid": [1, 2, 3, 4, 5, 6, 7, 8, 9], "name": ["a", "b", "c", "d", "e", "f", "g", "h", "j"]}
    df = pd.DataFrame(data)
    initial_size = 0.7
    initial_pair, insert_pair = split(dir_name, filename, initial_size)
    initial_fn, initial_df = initial_pair
    insert_fn, insert_df = insert_pair
    sort_key = 'sid'
    table = DBTable(file_name=initial_fn, index_levels=[1, 2, 4], sort_key=sort_key, init_depth=0, hash_size=5)
    
    #testing selection:
    print("Testing Selection Accuracy-----")
    accuracy = test_accuracy(initial_df, sort_key, table)
    print("Able to find : ", (accuracy))
    
    #testing insertion
    print("Testing Insertion-----")
    #inserting
    n = len(insert_df)
    for i in range(n):
        row = insert_df.iloc[i]
        table.insert(row)
    insert_accuracy = test_accuracy(insert_df, sort_key, table)
    print('Accuracy for selection of insertion items: ', insert_accuracy)
    
    