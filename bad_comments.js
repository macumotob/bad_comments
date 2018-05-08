
// beproduct Total :  244,776  Bad :  64,855  // doc.HeaderId === doc.CompanyId
const ERROR_DOCUMENT_ID = "one item";
var MongoClient = require('mongodb').MongoClient;

var url = "mongodb://localhost:27017/";
var database = "beproduct";
var backupCollectionName = "CommentsBackupUnique";


//var url = "mongodb://bedevuser:8uRt46Ghtp13@ds036299-a0.mlab.com:36299,ds036299-a1.mlab.com:36294/devbeproduct?replicaSet=rs-ds036299";
//var database = "devbeproduct";

var server = null;
var pages = [];
var appIDs = [];

var comments = [];
var dbo;
var uniq = [];
	
MongoClient.connect(url, function(err, client) {
  if (err) throw err;
  console.log("Connected");
  afterConnect(client);
  });

function afterConnect(client){
	
	server = client;
	
	var db = client.db(database);
	dbo = db;

    var collection = db.collection("Pages");
    	
	db.collection("Applications").find({ "MasterFolder": { $ne: "Request" }}).each(function(err, doc) {
		if (err) throw err;
	    if(doc){
			appIDs.push(doc._id);
		}
		else{
			var count = 0;
			console.log(appIDs.length);
					
			collection.find({ "ApplicationId": { $in: appIDs }, "TimelineId": { $ne: null } }).each(function(err, page) {
				if (err) throw err;
				if(page){
					pages.push(page); 
				}
				else{
					console.log("Pages total : " , count, " Empty :", pages.length);
		            findComments(db,null, function(){
						console.log("Pages length X == 0");
						console.log("Page-Comments : ", comments.length);
						checkComments();
					});
					//SaveEmptyPage(backupCollection);
					
				}				
			});
			
	}});
}
function findInUnique(page_id){
	for(var i = 0; i < uniq.length;i++){
        var item = uniq[i];
		if(item.page_id  == page_id){
			return item;
		}
	}
	return null;
}
function makeDocumentId(documentId) {

    var x = documentId.split("--");

    if (x.length == 3) {
        return x[0] + "--" + x[1];
    }
    else if (x.length == 2) {
        return x[0] + "--" + x[1];
    }
    else if (x.length == 1) {
        return ERROR_DOCUMENT_ID;
    }
    else {

    }
}
function checkComments(){

	comments.forEach( function(item){
		var uniqItem = findInUnique(item.page._id);
        if (uniqItem == null) {
            uniq.push({ page_id: item.page._id, page: item.page, comments: [item.comments] });
		}
		else{
			
			uniqItem.comments.push(item.comments);
		}
		//console.log(list);
		
	});
	//uniq.sort(function(a, b){
	//	if(a.page_id < b.page_id) return -1;
	//	if(a.page_id > b.page_id) return 1;
	//	return 0;
    //   });
    uniq.forEach(function (item) {
        item.comments.forEach(function (comment) {
            comment.OldDocumentId = comment.DocumentId;
            comment.DocumentId = makeDocumentId(comment.DocumentId)
            if (comment.DocumentId == ERROR_DOCUMENT_ID) {
                comment.DocumentId = item.page.HeaderId + "--" + item.page.ApplicationId;
                item.message = ERROR_DOCUMENT_ID;
            }
            });
    });
    saveUniqueComments();
}
function findComments(db,tbComments,callback){
	//console.log("Pages length : ",pages.length );
	if(pages.length == 0){
		callback();
		return;
	}
	if(!tbComments ){
		tbComments = db.collection("Comments");
	}
	var page = pages.pop();
	var query = { $or:[
    {"DocumentId" : page.HeaderId + "--" + page.ApplicationId + "--" + page.TimelineId},
	{"DocumentId" : page.HeaderId + "--" + page.ApplicationId},
	{"DocumentId" : page.HeaderId}
  ]};
    tbComments.find(query).each(function (err,comment) {
		if (err) throw err;
		//console.log(comment);
		if(comment){
            //console.log("comment id : " ,comment._id);	
		    comments.push({ page: page, comments : comment});
		}
		else{
			findComments(db,tbComments,callback);
		}
        //commentsUpdate.push({_id : page._id, DocumentId : page.HeaderId + "--" + page.ApplicationId});
        //printjson(comment);
    });
	
}

function close(){
  	server.close();
	//console.log("\n\n******** DONE ");
}

function saveUniqueComments(backupCollection){
   if(uniq.length == 0){
       close();
	   return;
    }
    if (!backupCollection) {
        backupCollection = dbo.collection(backupCollectionName);
    }
   var item = uniq.pop();
   
    backupCollection.insertOne(item, function(err, res) {
        if (err) throw err;
        //pagesForDelete.push(page._id);
          saveUniqueComments(backupCollection);
  });
}
function deleteEmptyPages1(collection){
   if(pagesForDelete.length == 0){
	   close(); 		
	   return;
    }
	if(!collection){
		collection = dbo.collection("Pages");
	}
	var id = pagesForDelete.pop();
	var query = {_id: id};
	console.log("delete : ", id);
	collection.deleteOne(query, function(err,result) {
		if (err) throw err;
		if(pagesForDelete.length > 0){
			deleteEmptyPages(collection);
		}
		else{
			close(); 		
		}
	});;
}
