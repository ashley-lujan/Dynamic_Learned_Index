from hasher import ExtensibleHash
import pandas as pd
from rmi import MultiLevelRMI


class DBTable:
    def __init__(self, file_name:str, sort_key: str, index_levels=[1, 4, 16], hash_size=10, init_depth = 0):
        self.file_name = file_name
        self.sort_key = sort_key
        self.hash_size = hash_size
        self.init_depth = init_depth
        self.schema, self.data, keys = self.load_data()
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
            row_val = df.iloc[i].values #just a list of values
            entity = ExtensibleHash(get_key_val=get_key_val, init_size=self.hash_size, init_depth= self.init_depth)
            entity.insert_item(row_val)
            data[i] = entity
            keys[i] = get_key_val(row_val)
        return schema, data, keys
    
    def select(self, key_val):
        pos, error, _ = self.li._predict_pos_and_error(key_val)
        #todo: look around error
        search_lim = error // 2
        i = 0
        while i < search_lim:
            pos_left = pos - i
            pos_right = pos + i
            #search on left
            if pos_left >= 0 and pos_left < len(self.data):
                data_container = self.data[pos_left]
                results = data_container.get(key_val)
                if results is not None:
                    return results
            #search on right
            if pos_right >= 0 and pos_right < len(self.data):
                data_container = self.data[pos_right]
                results = data_container.get(key_val)
                if results is not None:
                    return results
            i+= 1           
            
        return results
    
    
    def insert(self, row):
        '''
        Inserts row into table
        :param self: Description
        :param row: {"att1": val, "attr2": val2} of row being added to table
        '''
        key_val = row[self.sort_key]
        pos, error, _ = self.li._predict_pos_and_error(key_val)
        #TODO: ideally look within error bound to find the best match,
        # i.e a bucket really close to pos, but that also allows the data to be 'sorted'
        pos = min(len(self.data) - 1, pos)
        pos = max(0, pos)
        data_container = self.data[pos]
        data_container.insert_item(row.values)
            
        
        
        
        
        
        