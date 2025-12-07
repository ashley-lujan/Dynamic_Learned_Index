import re

def create_table(query):
    match = re.match(r"create table\s+(\w+)\s+columns:\s*\[(.*?)\]", query)
    if match:
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(",")]
    else:
        raise ValueError("String format not recognized")
    return table_name, columns

def select_entity(query):
    pattern = r"select from\s+(\w+)\s+where key\s*=\s*'([^']*)'"
    match = re.search(pattern, query)

    if match:
        table_name = match.group(1)
        key_value = match.group(2)
    else:
        raise ValueError("String not in expected format")
    return table_name, key_value

#TODO: should it load in in-memory value of Learned Index or relearn?
def connect(query):
    pattern = r"connect table\s+(\w+)"
    match = re.search(pattern, query)
    
    if match:
        table_name = match.group(1)
    else:
        raise ValueError("String not in expected format")
    return table_name

def insert(query):
    '''
    Returns table name and values for a new entity in the table
    :param query: insert table_name (key_val, attr1, attr2, ..., attr3)
    '''
    pattern = r"insert\s+(\w+)\s*\((.*?)\)"
    match = re.search(pattern, query)

    if not match:
        raise ValueError("Invalid format")

    table_name = match.group(1)
    values = tuple(v.strip() for v in match.group(2).split(","))
    return table_name, values
    
    
