from manager import DataManager
import parsers as parse
from enum import Enum

class Commands(Enum):
    create_table = "create table"
    show_table = "show table"
    insert = 'insert'
    delete = 'delete'
    select = 'select from'
    connect = 'connect'
    load_table = "load table"
    
class Examples(Enum):
    create_table = "create table cats columns: [key, color]"
    insert = 'insert cats (cat1, black)'
    delete = 'delete ?'
    select = "select from cats where sid = 'cat1'"
    connect = 'connect table cats'
    load_table = "load table sailors key = 'sid' depth_limit = 5"
    show_table = "show table sailors"
    


if __name__ == "__main__":
    db = DataManager()
    while True:
        query = input("sql>")
        if query.startswith(Commands.create_table.value):
            try:
                table_name, column_names = parse.create_table(query)
                db.create_table(table_name = table_name, column_names=column_names)
            except:
                print("Error parsing. I.e: ", Examples.create_table.value)
        elif query.startswith(Commands.select.value):
            # try:
                table_name, key_value = parse.select_entity(query)
                key_value = int(key_value)
                db.select(table_name=table_name, key_value=key_value)
            # except:
            #     print("Error parsing. I.e: ", Examples.select.value)
        elif query.startswith(Commands.connect.value):
            try:
                table_name = parse.connect(query)
                db.connect(table_name=table_name)
            except:
                print("Error parsing. I.e: ", Examples.connect.value)
        elif query.startswith(Commands.insert.value):
            # try:
                table_name, entity = parse.insert(query)
                db.insert(table_name, entity)
            # except:
            #     print("Error parsing. I.e: ", Examples.insert.value)
        elif query.startswith(Commands.load_table.value):
            try:
                table_name, key_name, limit = parse.load_table(query)
                db.load_table(table_name, key_name, int(limit))
            except:
                print("Error parsing. I.e : ", Examples.load_table.value)
        elif query.startswith(Commands.show_table.value):
            try:
                table_name = parse.show_table(query)
                schema = db.get_schema(table_name=table_name)
                print("For table:", table_name, ", schema:", schema)
            except:
                print("Error parsing. I.e: ", Examples.show_table.value)
        else:
            print("Invalid command")
            
            

            