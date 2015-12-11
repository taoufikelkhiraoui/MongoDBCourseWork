import string
import csv
import json
import pymongo
from pymongo import MongoClient
import datetime
from datetime import timedelta
from bson.code import Code

header= [ "id","id_member","timestamp","text","geo_lat","geo_lng"]
client = MongoClient('mongodb://taoufikds.cloudapp.net:27017/')
db = client.blogDb
collection = db.newBlogMessage

#6--------------------------------------------------------------------------------------------
#unigram
map    = Code("function map(){"
              "if(this.text == null ||  typeof this.text != 'string')"
              "return;"
              "var unigrams = this.text.match(/\w+/g);"
#             "var unigrams = this.text.split("" "");"
              "if(unigrams == null)"
              "{return;}"
              "for(var i=0;i<unigrams.length;i++)"
              "emit(unigrams[i] ,{count : 1});"
              "}")

reduce = Code("function reduce(key, values){"
               "var total_num = 0;"
               "for(var i= 0; i < values.length;i++)"
               "{total_num = values[i].count + total_num;}"
               "return total_num;"
               "}")

print '6/ '

results = collection.map_reduce(map, reduce, "myresults")

for res in results.find():
    a = res["value"]
    b = res["_id"]
    print b +  ' : ' +  a


print 'end '
