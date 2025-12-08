import pandas as pd
from table import DBTable

#The idea is that the DataManager holds the items in memory.
class DataManager:
    def __init__(self):
        self.tables = {} #tableName: DbTable
    
    # self __init__():
    def create_table(self, table_name, column_names: list):
        print(table_name, column_names)
    
    def get_schema(self, table_name):
        if table_name not in self.tables:
            return []
        return self.tables[table_name].schema
    
    def insert(self, table_name: str, entity: tuple):
        db_table = self.tables[table_name]
        row = {}
        for col_name, entity_val in zip(db_table.schema, entity):
            row[col_name] = [entity_val]
        df = pd.DataFrame(row)
        df[db_table.sort_key] = df[db_table.sort_key].apply(lambda x: int(x))
        row = df.iloc[0]
        db_table.insert(row)
        print("Inserted row: ", row, "to: ", table_name)
        
    
    #from table_name select where key = key_value
    def select(self, table_name, key_value):
        #returns the dictionary representation of the tuple
        db_table = self.tables[table_name]
        result = db_table.select(key_value)
        print('Result: ', result)

    def load_table(self, table_name:str, key_name):
        #read file name and save as hash indexes
        db_table = DBTable(file_name="data/original_csv/" +table_name + ".csv", sort_key=key_name, index_levels=[1, 4, 6], hash_size=10, init_depth=0)
        self.tables[table_name] = db_table
        print("Created table,", table_name ,"with schema: ", db_table.schema)
    
    def connect(self, table_name:str):
        pass
    

#Considerations for future, delete using key value and using tuple that matches it? 

    
        