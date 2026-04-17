            
from pathlib import Path
from uuid import uuid4
from datetime import date, datetime
from dataclasses import dataclass, asdict
from src.potatodb.db import PotatoDB

dpath = Path(__file__).parent     
dbf:str = Path.as_posix( Path.joinpath(dpath, 'data') )      
dbn:str = "fruits"

@dataclass
class Fruit():
    id:str = '' 
    name:str = ''
    amt:int = 0
    price:float= 0.0
    stockin_date:int = 0    
           
    @property
    def dump(self):
        #self.set_id
        return asdict(self)
        
apple = Fruit(id='T815', name="Tangerine", amt=365, price=6.00, stockin_date=1776363589000)

db = PotatoDB(dbf) # type: ignore
#db.create_table(dbn)
#print(f" created {result}")

db.insert("fruits", apple.dump )


#print(db.dump(apple.dump))

#print(db.report) 
#print( db.query("fruits", lambda record: True ) )
#print(db.hash_store(table_name='fruits'))
#Query all records above the age of 30 from the "users" table
#pear = db.query("fruits", lambda record: record['doc']["name"] == 'pear', lambda record: 'price' in record['doc'].keys() and record['doc'].get('price') < 20.00 )
#print(pear)
q1 = {"id":'P36R2501'}
q2 = {"name":'pear', "price": "< 20.00" }
pears = db.run_query("fruits", { "name": "pear" } )
tangi = db.run_query("fruits", { "id": "T815" } )
#all_fruits = db.query("fruits", lambda record: True)
#cheap_pear = db.run_query("fruits", q2 ) #{ "price": "< 20.00" } )
#cheap_fruits = db.run_query("fruits", { "price": "<= 8.00" } )

print( tangi[0] )        
        
        
    