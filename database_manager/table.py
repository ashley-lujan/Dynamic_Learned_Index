from hasher import ExtensibleHash
import pandas as pd
from rmi import MultiLevelRMI


class DBTable:
    def __init__(self, file_name:str|None, sort_key: str, index_levels=[1, 4, 16], hash_size=10, init_depth = 0, from_data=None, depth_limit = 5, log_modeling=False):
        self.file_name = file_name
        self.index_levels = index_levels
        self.log_modeling = log_modeling
        self.sort_key = sort_key
        self.hash_size = hash_size
        self.init_depth = init_depth
        self.depth_limit = depth_limit
        self.schema, self.data, keys = self.load_data(file_name, from_data)
        self.li = MultiLevelRMI(levels=index_levels)
        if self.log_modeling:
            print('Fitting Model')
        self.li.fit(keys)
        if self.log_modeling:
            print('Finished fitting model')
        
    
    def load_data(self, file_name, from_data):
        #returns data
        if file_name is None:
            if from_data is None:
                raise Exception("Can not construct a table with nothing!")
            df = from_data
        else:
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
    
    def flatten_entity(self, i):
        #convert ith position to another table
        hash_ds = self.data[i]
        hash_ds_data = hash_ds.get_data()
        inner_data = {}
        for col_i, col_name in enumerate(self.schema):
            inner_data[col_name] = hash_ds_data[:, col_i]
        df = pd.DataFrame(inner_data)
        
        inner_table = DBTable(from_data=df, file_name=None, sort_key=self.sort_key, index_levels=self.index_levels, hash_size=self.hash_size, init_depth=self.init_depth)
        self.data[i] = inner_table
    
    def select(self, key_val):
        pos, error, _ = self.li._predict_pos_and_error(key_val)
        #todo: look around error
        #force pos to be either 
        pos = min(len(self.data), pos)
        pos = max(0, pos)
        search_lim = error // 2
        i = 0
        while i <= search_lim:
            pos_left = pos - i
            pos_right = pos + i
            #search on left
            if pos_left >= 0 and pos_left < len(self.data):
                data_container = self.data[pos_left]
                if isinstance(data_container, DBTable):
                    results = data_container.select(key_val)
                    if results is not None:
                        return results
                else:
                    results = data_container.get(key_val)
                    if results is not None:
                        return results
            #search on right
            if pos_right >= 0 and pos_right < len(self.data):
                data_container = self.data[pos_right]
                if isinstance(data_container, DBTable):
                    results = data_container.select(key_val)
                    if results is not None:
                        return results
                else:
                    results = data_container.get(key_val)
                    if results is not None:
                        return results
            i+= 1           
            
        return None
    
    
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
        if isinstance(data_container, DBTable):
            data_container.insert(row)
        else:
            data_container.insert_item(row.values)
            if data_container.global_depth > self.depth_limit:
                self.flatten_entity(pos)
    
    def __str__(self,):
        #prints tree representation of itself:
        representation = ""
        for data_container in self.data:
            representation += "\n\t" + str(data_container)
        return representation
        
            
        
#testing dynamicness
def test_table_growth(accuracy_test, log=False):
    df = {
        "userid": [0, 20, 40, 60]
    }
    df = pd.DataFrame(df)
    db_table = DBTable(from_data=df, file_name=None, sort_key="userid", index_levels=[1, 2, 4], hash_size=2, depth_limit=2, log_modeling=log)
    accuracy = accuracy_test(tb = db_table, sequence=df["userid"])
    print('Pre-accuracy: ', accuracy)
    test_numbers = []
    for i in df['userid']:
        test_numbers += [j for j in range(i + 1, i + 20)]
    print('test numbers became: ', test_numbers)
    test_df = pd.DataFrame({"userid": test_numbers})
    for i in range(len(test_numbers)):
        row = test_df.iloc[i]
        db_table.insert(row)
    fin_acc = accuracy_test(tb = db_table, sequence=[i for i in range(60)])
    print('Final-accuracy', fin_acc)
    if log:
        print(db_table)
    #then check accuracy when it comes to selecting all the numbers
    
        
        
        
        
        