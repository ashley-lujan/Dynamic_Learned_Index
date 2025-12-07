import parsers

if __name__ == "__main__":
    query = "connect table table1"
    tn = parsers.connect(query)
    print(tn)
    