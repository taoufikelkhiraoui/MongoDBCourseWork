# MongoDBCourseWork
the queries.py file contain all the queries (except the sixth) written in python 

the query_6.py contain the unigram query but it still very slow.


to transform all the stored timestamp from string to data :

//on mongo shell : 

var cursor = db.newBlogMessage.find()

while (cursor.hasNext()) {

var doc = cursor.next();

db.newBlogMessage.update({_id : doc._id}, {$set : {timestamp : new Date(doc.timestamp)}})

}
