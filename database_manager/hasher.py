import hashlib
import numpy as np

class Bucket:
    def __init__(self, init_size, local_depth):
        self.local_depth = local_depth
        self.init_size = init_size
        self.memory = [None] * init_size
        self.size = init_size
        self.next_index = 0
    
    def is_full(self):
        return self.next_index >= self.size
    
    def insert(self, item):
        if self.is_full():
            return False
        self.memory[self.next_index] = item
        self.next_index += 1
    
    def retrieve_item(self, query_item, val_extractor):
        for item in self.memory:
            if item is not None and val_extractor(item) == query_item:
                return item
        return None
    
    def overflow(self, hasher, val_extractor):
        self.local_depth += 1
        other_bucket = Bucket(self.init_size, self.local_depth)
        updated_memory = [None] * self.init_size
        i = 0
        for item in self.memory:
            if item is None:
                break
            hash_val = hasher(val_extractor(item))
            if hash_val & (1 << (self.local_depth - 1)):
                other_bucket.insert(item)
            else:
                updated_memory[i] = item
                i += 1
        self.memory = updated_memory
        self.next_index = i
        return other_bucket
    
    def __str__(self, ):
        
        return "local depth:{}, {}".format(str(self.local_depth), self.memory)
        

class ExtensibleHash:
    def __init__(self, get_key_val, init_size = 10, init_depth = 0):
        #TODO: do something with init depth
        #Memory is a list of lists
        self.memory = [Bucket(init_size, init_depth)]
        self.get_key_val = get_key_val
        #represent the bucket index it a memory point should refer to
        self.references = [0]
        self.global_depth = init_depth
        self.min = None
        self.max = None
            
    def normalize_for_hash(self, value):
        if isinstance(value, (np.generic,)):  
            value = value.item()

        # Convert Python native types to string
        if isinstance(value, (int, float, str)):
            return str(value)

        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                return value.hex()

        if value is None:
            return "None"
        return str(value)

    
    def hash(self, value):
        value = self.normalize_for_hash(value)
        return int(hashlib.sha256(value.encode('utf-8')).hexdigest(), 16)
    
    def get_index(self, val):
        return self.hash(val) % (2 ** self.global_depth)
    
    def double_buckets(self):
        self.global_depth += 1
        self.memory += ([None] * len(self.memory))
        self.references += [i for i in range(len(self.references))]
    
    def get_bucket(self, index):
        while self.references[index] != index:
            index = self.references[index]
        return index, self.memory[index]
    
    def overflow_bucket(self, bucket_index, bucket: Bucket):
        overflow_bucket_id = bucket_index + (2 ** bucket.local_depth)
        self.references[overflow_bucket_id] = overflow_bucket_id
        overflow_bucket = bucket.overflow(hasher = self.hash, val_extractor=self.get_key_val)
        self.memory[overflow_bucket_id] = overflow_bucket     
            
    
    def insert_item(self, item):
        '''
        Docstring for insert_item
        
        :param self: Description
        :param item: Description
        '''
        val = self.get_key_val(item)
        bucket_index = self.get_index(val)
        search_index, bucket = self.get_bucket(bucket_index)
        if bucket.is_full():
            if bucket.local_depth == self.global_depth:
                self.double_buckets()
            else:
                self.overflow_bucket(search_index, bucket) #this shouldn't be here quite yet?
            self.insert_item(item) #keeps doubling as long as space is required
        else:
            #update self.values
            bucket.insert(item)    
    
    def get_depth(self):
        return self.global_depth
    
    def get(self, key_val):
        bucket_index = self.get_index(key_val)
        search_index, bucket = self.get_bucket(bucket_index)
        return bucket.retrieve_item(key_val, self.get_key_val)
    
    def __str__(self, ):
        bucket_info = ""
        for i, bucket in enumerate(self.memory):
            bucket_info += "\n {}: {}".format(i, bucket)
        return "Global Depth: {}\n References: {}".format(self.global_depth, self.references) +  "Buckets: {}".format(bucket_info)

def basic_test(num = 10):
    hash_ds = ExtensibleHash(init_size = 2)
    for i in range(num):
        # print('insert({})------------'.format(i))
        hash_ds.insert_item(i)
        found_items = set()
        for bucket_i, buckets in enumerate(hash_ds.memory):
            if buckets is None:
                continue
            atNone = False
            for i, bucket_item in enumerate(buckets.memory):
                if bucket_item is None:
                    atNone = True
                elif atNone:
                    print("added a value after a None?? at bucket: {}".format(bucket_item))
                    print(hash_ds)
                    raise Exception('test error')
                elif bucket_item in found_items:
                    print("Found duplicate item in non duplicate test at bucket i: ", bucket_item) 
                    raise Exception('test error')
                else:
                    found_items.add(bucket_item)
    print("PASSED BASIC TEST with Num = ", num)
    
def custom_test(sequence):
    hash_ds = ExtensibleHash(init_size = 2)
    for i in sequence:
        # print('insert({})------------'.format(i))
        hash_ds.insert_item(i)
        found_items = set()
        for bucket_i, buckets in enumerate(hash_ds.memory):
            if buckets is None:
                continue
            atNone = False
            for i, bucket_item in enumerate(buckets.memory):
                if bucket_item is None:
                    atNone = True
                elif atNone:
                    print("added a value after a None?? at bucket: {}".format(bucket_item))
                    print(hash_ds)
                    raise Exception('test error')
                elif bucket_item in found_items:
                    print("Found duplicate item in non duplicate test at bucket i: ", bucket_item) 
                    raise Exception('test error')
                else:
                    found_items.add(bucket_item)
    if len(found_items) == len(sequence):
        print("PASSED COSTUME TEST with input = ", sequence)
    else:
        print("FAIL: ONLY FOUND NUMBERS: ", found_items)
