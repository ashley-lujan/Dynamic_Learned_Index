import re

def create_table(query):
    match = re.match(r"create table\s+(\w+)\s+columns:\s*\[(.*?)\]", query)
    if match:
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(",")]
    else:
        raise ValueError("String format not recognized")
    return table_name, columns

def load_table(query):
    pattern = r"^\s*load table\s+([A-Za-z_][A-Za-z0-9_]*)\s+key\s*=\s*'([A-Za-z_][A-Za-z0-9_]*)'(?:\s+depth_limit=(\w+))?\s*$"
    match = re.match(
        pattern,
        query
    )

    if match:
        table_name = match.group(1)
        key_name = match.group(2)
        depth_raw = match.group(3)
        if depth_raw is None:
            depth_limit = None
        elif depth_raw.isdigit():
            depth_limit = int(depth_raw)
        elif depth_raw == "None":
            depth_limit = None
        else:
            raise ValueError(f"Unexpected depth_limit value: {depth_raw}")
    else:
        raise ValueError("String format not recognized")
    return table_name, key_name, depth_limit

def show_table(query):
    match = re.match(
        r"^\s*show table\s+([A-Za-z_][A-Za-z0-9_]*)\s*",
        query
    )

    if match:
        table_name = match.group(1)
    else:
        raise ValueError("String format not recognized")
    return table_name

def select_entity(query):
    pattern = r"select from\s+([A-Za-z_][A-Za-z0-9_]*)\s+where\s+key\s*=\s*'([A-Za-z0-9_]+)'"
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
    pattern = r"insert\s+([A-Za-z_][A-Za-z0-9_]*)\s*\((.*?)\)"
    match = re.search(pattern, query)

    if not match:
        raise ValueError("Invalid format")

    table_name = match.group(1)
    values = tuple(v.strip() for v in match.group(2).split(","))
    return table_name, values
    
    
