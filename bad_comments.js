
// beproduct Total :  244,776  Bad :  64,855  // doc.HeaderId === doc.CompanyId
const ERROR_DOCUMENT_ID = "one item";
const NULL_DOCUMENT_ID = "NULL DOC ID";
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
var commentsOnly = [];	

var commentsStatus = [];	
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
        return NULL_DOCUMENT_ID;
    }
}
//function syncComments(item) {
//    var main = item.comments[0];
//    item.comments.forEach(function (comment) {
//        if (comment != main) {
//            comment.status = "delete";
//            comment.Replies.forEach(function (rep) {
//                rep.moved = true;
//                main.Replies.push(rep);
//            });
//        }
//    });
//    main.Replies.sort( function(a, b) {
//        return a.CreateDate == b.CreateDate ? 0
//            : a.CreateDate > b.CreateDate ? -1 : 1;
//    });
//    main.status = "synced";
//}
function syncComments(comments) {
    if (comments.length == 1) {
        
        comments[0].status = "update";
        commentsStatus.push(comments[0]);
        return;
    }
    var main = comments[0];
    comments.forEach(function (comment) {
        commentsStatus.push(comment);
        if (comment != main) {
            
            comment.status = "delete";
            comment.Replies.forEach(function (rep) {
                rep.moved = " moved from :" + comment._id + " to :" + main._id;
                main.Replies.push(rep);
            });
        }
    });
    main.Replies.sort(function (a, b) {
        return a.CreateDate == b.CreateDate ? 0
            : a.CreateDate > b.CreateDate ? -1 : 1;
    });
    main.status = "update";
}

function groupByDocumentId(db) {
    var data = [];
    var tb = db.collection(backupCollectionName);
    tb.aggregate([
        {
            $unwind: {
                path: "$comments",
                preserveNullAndEmptyArrays: true
            }
        },
        {
            $group: {
                _id: "$comments.DocumentId",
                count: { $sum: 1 }
            }
        }
        ,
        //{ $match: { count: { $gt: 1 } } }
    ], function (err, result) {
        if (err) {
            console.log(err);
        }
        else {
            result.each(function (e, item) {
                if (e) throw e;
                if (item) {
                    console.log(item);
                    data.push({ documentId: item._id, count: item.count });
                }
                else {
                    makeCommentsStatus(data);

                    db.collection("CommentsForUpdate").insertMany(commentsStatus, function (er, result) {
                        if (er) throw er;
                        close();
                        //commentsStatus.forEach(function (x) {
                        //    prepareForSave(x);
                        //});
                        //updateWorkingComments(db.collection("Comments"), function () {
                        //    console.log("end");//
                        //    close();
                        //});
                        
                    });
                }
            });
        }
    }
    );
}
function makeCommentsStatus(data) {
    data.forEach(function (item) {
        var list = commentsOnly.filter(function (e, i, a) {
            return e.DocumentId == item.documentId;
            });
        syncComments(list);
    });

}
function updateWorkingComments(col,callback) {
    if (commentsStatus.length == 0) {
        callback();
        return;
    }
    var comment = commentsStatus.pop();
    if (comment.status == "update") {
        prepareForSave(comment);
        var query = { _id: comment._id };
        col.replaceOne(query, comment);
        updateWorkingComments(col, callback);
        //col.replaceOne(query, comment, function (err, result) {
        //    if (err) throw err;
        //    updateWorkingComments(col, callback);
        //});
    }
    else if (comment.status == "delete") {
        prepareForSave(comment);
        var query = { _id: comment._id };
       
        col.deleteOne(query);
        updateWorkingComments(col, callback);
        //col.deleteOne(query,null, function (err, result) {
        //    if (err) throw err;
        //    updateWorkingComments(col, callback);
        //});
    }

}
function prepareForSave(comment) {
    delete comment["OldDocumentId"];
    delete comment["status"];
    comment.Replies.forEach(function (rep) {
        delete rep["moved"];
    });
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
            //comment.message = "";
            comment.OldDocumentId = comment.DocumentId;
            comment.DocumentId = makeDocumentId(comment.DocumentId)
            //if (comment.DocumentId == ERROR_DOCUMENT_ID) {
            //    comment.DocumentId = item.page.HeaderId + "--" + item.page.ApplicationId;
            //    comment.message = ERROR_DOCUMENT_ID;
            //}
            //else if (comment.DocumentId == NULL_DOCUMENT_ID) {
            //    comment.message = NULL_DOCUMENT_ID;
            //}
            //if (item.comments.length == 0) {
            //    comment.status = "error";
            //}
            //else if (item.comments.length == 1) {
            //    comment.status = "update";
            //}
            //else {
            //    comment.status = "sync";
            //}
        });
        //if (item.comments.length > 1) {
        //    item.status = "sync";
        //    syncComments(item);
        //}
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
	//{"DocumentId" : page.HeaderId}
  ]};
    tbComments.find(query).each(function (err,comment) {
		if (err) throw err;
		//console.log(comment);
		if(comment){
            //console.log("comment id : " ,comment._id);	
            if (comment.Type == "Like") {
                comments.push({ page: page, comments: comment });
            }
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
    if (uniq.length == 0) {
        groupByDocumentId(dbo);
       //close();
	   return;
    }
    if (!backupCollection) {
        backupCollection = dbo.collection(backupCollectionName);
    }
    var item = uniq.pop();
    item.comments.forEach(function (com) {
        commentsOnly.push(com);
    } );
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
