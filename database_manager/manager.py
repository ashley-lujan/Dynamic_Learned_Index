import pandas as pd

#The idea is that the DataManager holds the items in memory.
class DataManager:
    def __init__(self):
        #learned index --> Learned Index object that on .predict() returns hash index
        tables = [] #list of tables, tables are dictionaries
    
    # self __init__():
    def create_table(self, table_name, column_names: list):
        print(table_name, column_names)
    
    def get_schema(self, table_name):
        return self.tables[table_name]
    
    def insert(self, table_name: str, entity: tuple):
        pass
    
    #from table_name select where key = key_value
    def select(self, table_name, key_value):
        #returns the dictionary representation of the tuple
        return None

    def load(self, table_name:str):
        #read file name and save as hash indexes
        pass
    
    def connect(self, table_name:str):
        pass
    

#Considerations for future, delete using key value and using tuple that matches it? 

    
        