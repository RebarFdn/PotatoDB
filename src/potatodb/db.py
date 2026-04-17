import os
import json
import hashlib
import operator
from uuid import uuid4
from datetime import date, datetime
from functools import lru_cache

# Map the string to the function
ops = {">": operator.gt, "<": operator.lt, "==": operator.eq, "<=": operator.le, ">=": operator.ge}


class PotatoDB:
    
    def __init__(self, folder="LazyDB"):
        self.tables = {}
        self.set_folder(folder)
        self.load()


    def create_table(self, table_name:str):
        """Creates a new table."""
        self.tables[table_name] = []
        self.save(table_name)


    def insert(self, table_name:str, data:dict):
        """Inserts a new record only if it doesn't already exist."""
        temp = None
        if table_name in self.tables:            
            if not self.check_exist(table_name=table_name, data=data):  # Check if exact match exists
                temp = self.dump(data)
                self.tables[table_name].append(temp)
                # store the hash
                # self.store_hash(hash=temp.get("hash"))
            else:
                return None  # Duplicate detected
        else:
            self.tables[table_name] = [data]
        
        self.save(table_name)
        del temp
        return data


    def query(self, table_name:str, query_func, deep_query_func=None):
        """Queries data from the specified table using a query function."""
        if table_name in self.tables:
            result = list(filter(query_func, self.tables[table_name]))
            if deep_query_func:
                result = list(filter(deep_query_func, result)) 
            return result
        else:
            return None


    def update(self, table_name:str, condition_func, update_func):
        """Updates records in the specified table based on a condition."""
        if table_name in self.tables:
            for record in self.tables[table_name]:
                if condition_func(record):
                    update_func(record)
            self.save(table_name)
            return True
        else:
            return False


    def delete(self, table_name:str, condition_func):
        """Deletes records from the specified table based on a condition."""
        if table_name in self.tables:
            self.tables[table_name] = [record for record in self.tables[table_name] if not condition_func(record)]
            self.save(table_name)
            return True
        else:
            return False


    def set_folder(self, folder_name:str):
        """Sets the folder where the tables will be saved and loaded."""
        self.folder = folder_name
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)
        return True


    def save( self, table_name:str | None = None ):
        """Saves the specified table to a JSON file in the set folder, using the table name as the filename."""
        if table_name:
            if table_name in self.tables:
                filename = os.path.join(self.folder, f"{table_name}.json")
                with open(filename, 'w') as file:
                    json.dump(self.tables[table_name], file, indent=4)
            else:
                raise ValueError(f"Table '{table_name}' does not exist.")
        else:
            # Save all tables to the folder
            for table_name in self.tables:
                filename = os.path.join(self.folder, f"{table_name}.json")
                with open(filename, 'w') as file:
                    json.dump(self.tables[table_name], file, indent=4)

    
    def load( self, table_name:str | None = None ):
        """Loads a table from a JSON file in the set folder. If table_name is not specified, it loads all tables."""
        if self.folder:
            if table_name:
                filename = os.path.join(self.folder, f"{table_name}.json")
                if os.path.exists(filename):
                    with open(filename, 'r') as file:
                        self.tables[table_name] = json.load(file)
                    return True
                else:
                    raise ValueError(f"Table '{table_name}' does not exist.")
            else:
                # Load all tables from the folder
                for file in os.listdir(self.folder):
                    if file.endswith('.json'):
                        table_name = os.path.splitext(file)[0]
                        with open(os.path.join(self.folder, file), 'r') as json_file:
                            self.tables[table_name] = json.load(json_file)
        else:
            raise ValueError("Folder not set. Use set_folder method to set the folder path.")
    
    
    @property
    def timestamp(self):
        return int(datetime.now().timestamp()) * 1000

    
    @property    
    def is_empty( self ):
        if self.tables:
            for item in self.tables.keys():
                test = self.query(item, lambda record: True )
                if test:
                    return False
                else:
                    return True
        else:
            return True
        
    
    @property
    def report( self ):
        '''Generate Database Statistics Report'''
        report_data:dict = dict( 
            name = 'PotatoDB',
            directory = self.folder,
            tables = list( self.tables.keys() )
        )
        report_data['contents'] = dict( 
            tables = len( report_data['tables']),
            data_sets = 0 
        )
        for table in report_data.get('tables', []):
            report_data['contents']['data_sets'] += len(self.tables.get(table, []))
        return report_data  
    
    
    @lru_cache(maxsize=356) # Hash Store
    def hash_store(self, table_name:str):
        '''Rudimentary method to retreive hashes'''
        dbase:list = self.query(table_name, lambda record: True ) # type: ignore
        try:
            return [item.get('hash') for item in dbase ]
        except:
            return []
        finally:
            del dbase
        
        
    def hash_json(self, data:dict):
        ''' Returns a hash of the data for integrity checks '''
        return hashlib.shake_256(json.dumps(data, sort_keys=True).encode()).hexdigest(16)
        
    
    def check_exist(self, table_name:str, data:dict)->bool:
        test_item = self.hash_json(data=data)
        try:
            if test_item in self.hash_store(table_name=table_name):
                return True
            return False
        finally:
            del test_item     
        
    
    def dump(self, data:dict):
        if '_id' in data.keys():
            data["hash"] = self.hash_json(data=data.get('doc', {}))
            data["updated"] = self.timestamp
            return data
        else:
            return {
                "_id": uuid4().hex,
                "hash": self.hash_json( data=data),
                "updated": self.timestamp,
                "doc": data
            }   
        
    
    def run_query(self, table_name:str, query_data:dict={}):
        """Queries data from the specified table using a query dictionary."""
        if table_name in self.tables:
            keys:list = list(query_data.keys())
            if keys:
                if len(keys) > 1:
                    comp, value = tuple(query_data[keys[1]].split(' '))
                    first_run = list(filter(lambda record: record['doc'][keys[0]] == query_data[keys[0]], self.tables[table_name]))
                    result = list(filter(lambda record: keys[1] in record['doc'].keys() and ops[comp](record['doc'].get(keys[1]), float(value)), first_run)) 
                    return result
                elif len(keys) == 1:
                    key_value = list(query_data[keys[0]].split(' '))
                    if len(key_value) > 1:
                        comp2, val = tuple(key_value)
                                                
                        first = list(filter( lambda record: True, self.tables[table_name]))
                        return list(filter(lambda record: keys[0] in record['doc'].keys() and ops[comp2](record['doc'].get(keys[0]), float(val)), first))
                    else: 
                        return list(filter(lambda record: record['doc'][keys[0]] == query_data[keys[0]], self.tables[table_name]))                    
        else:
            return None
        
