from hasher import ExtensibleHash
import pandas as pd

class DBTable:
    def __init__(self, file_name:str, sort_key: str, hash_size=10, init_depth = 0):
        self.file_name = file_name
        self.schema, self.data = self.load_data()
        self.sort_key = sort_key
        self.hash_size = hash_size
        self.init_depth = init_depth
    
    def load_data(self):
        #returns data
        df = pd.read_csv(self.file_name)
        n = len(df)
        data = [None] * n
        schema = df.columns.tolist()
        df = df.sort_values(self.sort_key)
        
        sort_key_index = schema.index(self.sort_key)        
        def get_key_val(tuple, i=sort_key_index):
            return tuple[i]
        
        for i in range(n):
            row_val = df.loc[i].values #just a list of values
            entity = ExtensibleHash(self.hash_size, self.init_depth, get_key_val)
            entity.insert_item(row_val)
            data[i] = entity
        return schema, data
        
        