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
    return initial, insert
    

if __name__ == "__main__":
    dir_name = 'data/original_csv/'
    filename = 'sailors'
    
    data = {"uid": [1, 2, 3, 4, 5, 6, 7, 8, 9], "name": ["a", "b", "c", "d", "e", "f", "g", "h", "j"]}
    df = pd.DataFrame(data)
    initial_size = 0.7
    initial, insert = split(dir_name, filename, initial_size)
    
    table = DBTable(file_name=initial, sort_key='sid', init_depth=0, hash_size=5)
    #todo: insert
    
    
    
    
    # table = DBTable()