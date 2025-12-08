import hashlib
import random

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
    
    def retrieve_item(self, query_item):
        for item in self.memory:
            if item is not None and query_item == item:
                return item
        return None
    
    def overflow(self):
        self.local_depth += 1
        other_bucket = Bucket(self.init_size, self.local_depth)
        updated_memory = [None] * self.init_size
        i = 0
        for item in self.memory:
            if item is None:
                break
            if item % (2 ** self.local_depth):
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
    def __init__(self, init_size = 10, init_depth = 0):
        #TODO: do something with init depth
        #Memory is a list of lists
        self.memory = [Bucket(init_size, init_depth)]
        #represent the bucket index it a memory point should refer to
        self.references = [0]
        self.global_depth = init_depth
    
    def hash(self, value):
        # return hashlib.sha256(value)
        return value
    
    def get_index(self, item):
        return self.hash(item) % (2 ** self.global_depth)
    
    def double_buckets(self):
        self.global_depth += 1
        self.memory += ([None] * len(self.memory))
        self.references += [i for i in range(len(self.references))]
        print('duplicate myself', self)
    
    def get_bucket(self, index):
        while self.references[index] != index:
            index = self.references[index]
        return index, self.memory[index]
    
    def overflow_bucket(self, bucket_index, bucket: Bucket):
        overflow_bucket_id = bucket_index + (2 ** bucket.local_depth)
        self.references[overflow_bucket_id] = overflow_bucket_id
        overflow_bucket = bucket.overflow()
        self.memory[overflow_bucket_id] = overflow_bucket     
            
    
    def insert_item(self, item):
        '''
        Docstring for insert_item
        
        :param self: Description
        :param item: Description
        '''
        bucket_index = self.get_index(item)
        search_index, bucket = self.get_bucket(bucket_index)
        if bucket.is_full():
            if bucket.local_depth == self.global_depth:
                self.double_buckets()
            self.overflow_bucket(search_index, bucket)
            self.insert_item(item) #keeps doubling as long as space is required
        else:
            bucket.insert(item)    
    
    def get_depth(self):
        return self.global_depth
    
    def __str__(self, ):
        bucket_info = ""
        for i, bucket in enumerate(self.memory):
            if bucket is None:
                break
            bucket_info += "\n {}: {} \n".format(i, bucket)
        return "Global Depth: {}\n References: {}".format(self.global_depth, self.references) +  "Buckets: {}".format(bucket_info)
    
if __name__ == "__main__":
    #testing:
    hash_ds = ExtensibleHash(init_size = 2)
    for i in range(10):
        print('insert({})------------'.format(i))
        hash_ds.insert_item(i)
        print(hash_ds)