from manager import DataManager
import parsers as parse
import re
from enum import Enum

class Commands(Enum):
    #create table cat columns: [name, color]
    create_table = "create table"
    insert = 'insert'
    delete = 'delete'
    select = 'select from'
    connect = 'connect'


if __name__ == "__main__":
    db = DataManager()
    while True:
        query = input("sql>")
        if query.startswith(Commands.create_table.value):
            table_name, column_names = parse.create_table(query)
            db.create_table(table_name = table_name, column_names=column_names)
        elif query.startswith(Commands.select.value):
            table_name, key_value = parse.select_entity(query)
            db.select(table_name=table_name, key_value=key_value)
        elif query.startswith(Commands.connect.value):
            table_name = parse.connect(query)
            db.connect(table_name=table_name)
        elif query.startswith(Commands.insert.value):
            table_name, entity = parse.insert(query)
            db.insert(table_name, entity)
            

            