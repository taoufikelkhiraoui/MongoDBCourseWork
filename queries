
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



#1------------------------------------------------------------------------------------------
list = collection.find({}).distinct("id_member")
for var in list:
 if var < 0 :
  list.remove(var)
print '1/ Number of unique users : ',len(list)

#2-------------------------------------------------------------------------------------------
total_num = collection.count()
pipeline2 = [
              {"$group"   : {"_id" : "$id_member" ,"num_messages_per_user" : {"$sum" : 1}}},
              {"$sort"    : {"num_messages_per_user" : -1}},
              {"$limit"   : 10},
              {"$project" : {"num_messages_per_user" : 1 }} ]

result  = collection.aggregate(pipeline2)
total = 0
for num in result :
 total =   num["num_messages_per_user"] + total

final_result = (float(total)/total_num)*100
print '2/ Percentage of tweets twited by the top 10 users: %.2f%% ' %(final_result)

#3-------------------------------------------------------------------------------------------
pipeline3 = [
              {"$group"      : {"_id" : "null", "min_Date" : {"$min" : "$timestamp"}}},
              {"$project"    : {"min_Date" : 1}}]
pipeline4 = [
              {"$group"      : {"_id" : "null", "max_Date" : {"$max" : "$timestamp"}}},
              {"$project"    : {"max_Date" : 1}}]

min_result  = collection.aggregate(pipeline3)
for var in min_result :
 a = var["min_Date"]
 print '3.1/  The earliest date when a message was published : ' ,a

max_result = collection.aggregate(pipeline4)
for var in max_result:
 b = var["max_Date"]
 print '3.2/  The latest date when a message was published   : ' ,b

#4------------------------------------------------------------------------------------------

pipeline5 = [
              {"$project" : {"timestamp" : 1 }},
	      {"$sort"    : {"timestamp" : -1}}]
print '4/ Computing .... '

#result = collection.find().sort('timestamp', pymongo.DESCENDING)
#or
result = collection.aggregate(pipeline5,allowDiskUse=True)

values = []
for res in result:
  values.append(res["timestamp"])

timedeltas = [values[i-1]-values[i] for i in range(1, len(values))]
average = sum(timedeltas, datetime.timedelta(0)) / len(timedeltas)

print  '   The mean time delta between messages :' ,average

#5---------------------------------------------------------------------------------------------
map = Code("function map(){"
	    "if( this.text == null){"
	    "	return;"
	    "}"
	    "length = this.text.length;"
	    "emit(0 , length);"
	    "}")

reduce = Code("function reduce(key, values){"
	       "var total_num = 0;"
	       "for(var i= 0; i < values.length;i++)"
	       "{total_num = values[i] + total_num;}"
	       "return total_num;"
	       "}")

print '5/ Computing ...'
results = collection.map_reduce(map, reduce, "myresults")
total_num = collection.count()
for res in results.find():
    a = res["value"]
    print '   The mean length of a message: %.2f '  %(float(a)/total_num)


#6--------------------------------------------------------------------------------------------
#unigram
#map    = Code("function map(){"
         #     "if(this.text == null ||  typeof this.text != 'string')"
	#      "return;"
	   #   "var unigrams = this.text.match(/\w+/g);"
	  #    "var unigrams = this.text.split("" "");"
	 #     "if(unigrams == null)"
	#      "{return;}"
       #       "for(var i=0;i<unigrams.length;i++)"
      #        "emit(unigrams[i] ,{count : 1});"
     #         "}")

#reduce = Code("function reduce(key, values){"
 #              "var total_num = 0;"
  #             "for(var i= 0; i < values.length;i++)"
   #            "{total_num = values[i].count + total_num;}"
    #           "return total_num;"
     #          "}")

print '6/ '

#results = collection.map_reduce(map, reduce, "myresults")

#for res in results.find():
 #   a = res["value"]
  #  b = res["_id"]
  #  print b +  ' : ' +  a


#print 'end '

#7---------------------------------------------------------------------------------------
map = Code("function map(){"
            "if( this.text == null){"
            "   return;"
            "}"
            "if (typeof this.text == 'string')"
            "{var count = (this.text.match(/\#/g) || []).length;}"
	    "else return ;"
            "emit(0 , count);"
            "}")

reduce = Code("function reduce(key, values){"
               "var total_num = 0;"
               "for(var i= 0; i < values.length;i++)"
               "{total_num = values[i] + total_num;}"
               "return total_num;"
               "}")

print '7/ Computing ... '

results = collection.map_reduce(map, reduce, "myresults")
total_num = collection.count()

for res in results.find():
    a = res["value"]
    print '   The average number of Hashtags in a message: %.2f '  %(float(a)/total_num)

#8------------------------------------------------------------------------------------------------------------------
pipeline = [  ## match the messages within the UK
              {"$project" : {"geo_lat" : 1, "geo_lng" : 1}},
	      {"$match"   : {"$and": [ { "geo_lat": { "$gt": 49.776117, "$lt": 61.099621 } }, { "geo_lng": { "$gt": -11.005504, "$lt": 2.353889 } } ]}}, 
              {"$group"   : {"_id" : {"geo_lat": "$geo_lat" ,"geo_lng":"$geo_lng"}, "number_messages" :{"$sum":1}}},              
              {"$sort"    : {"number_messages" : -1}},
              {"$limit"   : 1}
 ]

print '8/ Computing ... '
result  = collection.aggregate(pipeline,allowDiskUse=True)

for res in result:
    lng = res["_id"]["geo_lng"]
    lat = res["_id"]["geo_lat"]
    num = res["number_messages"]
    print '   The area within the UK that contains the largest number of  messages : ( %f , %f ) with %d messages '  %(lat,lng,num)




