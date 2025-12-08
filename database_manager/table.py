from hasher import ExtensibleHash
import pandas as pd
from rmi import MultiLevelRMI


class DBTable:
    def __init__(self, file_name:str, sort_key: str, index_levels=[1, 4, 16], hash_size=10, init_depth = 0):
        self.file_name = file_name
        self.schema, self.data, keys = self.load_data()
        self.sort_key = sort_key
        self.hash_size = hash_size
        self.init_depth = init_depth
        self.li = MultiLevelRMI(levels=index_levels)
        print('Fitting Model')
        self.li.fit(keys)
        print('Finished fitting model')
        
    
    def load_data(self):
        #returns data
        df = pd.read_csv(self.file_name)
        n = len(df)
        data = [None] * n
        keys = [None] * n
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
            keys[i] = get_key_val(row_val)
        return schema, data, keys
    
    def select(self, key_val):
        pos = self.li.lookup(key_val)
        #todo: look around error
        data_container = self.data[pos] #Bucket
        results = data_container.get(pos)
        return results
        
        
        