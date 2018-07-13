//----------------------------------------------------
function findInUnique(page_id){
	for(var i = 0; i < uniq.length;i++){
        var item = uniq[i];
		if(item.page_id  == page_id){
			return item;
		}
	}
	return null;
}
//---------------------------------------------------
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
//---------------------------------------------------
function groupByDocumentId() {

    var result  = db.Comments_Unique.aggregate([
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
    ]).toArray();
    var data = [];
    for(var i =0;i < result.length;i++)
    {
	    var item = result[i];
        data.push({ documentId: item._id, count: item.count });
    }
    makeCommentsStatus(data);
//    printjson(data);
}
//------------------------------------------
function makeCommentsStatus(data) {
//*    print("data : " ,data.length);
//*    print("commentsOnly : " , commentsOnly.length);
//*    printjson(data);
//*    printjson(commentsOnly);
    
    data.forEach(function (item) {
        var list = [];
        for(var i=0; i < commentsOnly.length;i++){
          printjson(commentsOnly[i]);
          if(commentsOnly[i].DocumentId == item.documentId){
            list.push(commentsOnly[i]);
          }
        } 
/*        var list = commentsOnly.filter(function (e, i, a) {
            return e.DocumentId == item.documentId;
            });*/
    
        syncComments(list);
    });
    db.CommentsForUpdate.drop();
    db.CommentsForUpdate.insert(commentsStatus);

}
//------------------------------------------
function syncComments(comments) {

    if(!comments || comments.lenght == 0)
    {
       return;
    }
    if (comments.length == 1) {
        comments[0].status = "update";
        commentsStatus.push(comments[0]);
        print("3"); 
        return;
    }
    var xmain = comments[0];
    comments.forEach(function (comment) {
        commentsStatus.push(comment);
        if (comment != xmain) {
            
            comment.status = "delete";
            comment.Replies.forEach(function (rep) {
                rep.moved = " moved from :" + comment._id + " to :" + xmain._id;
                xmain.Replies.push(rep);
            });
        }
    });
    xmain.Replies.sort(function (a, b) {
        return a.CreateDate == b.CreateDate ? 0
            : a.CreateDate > b.CreateDate ? -1 : 1;
    });
    xmain.status = "update";
}

//---------------------------------------
    use beproduct;

    var appIDs = [];
    
	var commentsOnly = [];	
	var commentsStatus = [];	
    //------------------------------------
    // select app id
    //-------------------------------------
    db.Applications.find({ "MasterFolder": { $ne: "Request" }}).forEach(function(doc){
		 appIDs.push(doc._id);
    });
//*    print("APP ID COUNT:", appIDs.length);
    
    //------------------------------------
    // select pages
    //-------------------------------------
    var pages = [];
    db.Pages.find({ "ApplicationId": { $in: appIDs }, "TimelineId": { $ne: null } }).forEach(function(page) {
      pages.push(page);
    });
    
//*    print("Pages  count : " ,pages.length);
    
    //--------------------------------------------
    // select comments
    //--------------------------------------------
    var commentsUpdate = [];
    var comments = [];
    
    for(var i = 0; i < pages.length;i++){
      var page = pages[i];
      var query = { $or:[
	    {"DocumentId" : page.HeaderId + "--" + page.ApplicationId + "--" + page.TimelineId},//, "Type" : "Like"},
		{"DocumentId" : page.HeaderId + "--" + page.ApplicationId}//, "Type" : "Like"},
 	  ]};
	  db.Comments.find(query).forEach(function (comment){
    	  if (comment.Type == "Like") {
        	  comments.push({ page: page, comments: comment });
	      }
	      commentsUpdate.push({_id : page._id, DocumentId : page.HeaderId + "--" + page.ApplicationId});
	  });
    }
    db.Comments_Like.drop();
    db.Comments_Like.insert(comments);

//*    printjson(commentsUpdate);
    
   
    //-----------------------------------------
    //    make unique 
    //----------------------------------------
    var uniq = [];
    comments.forEach( function(item){
		var uniqItem = findInUnique(item.page._id);
        if (uniqItem == null) {
            uniq.push({ page_id: item.page._id, page: item.page, comments: [item.comments] });
		}
		else{
			
			uniqItem.comments.push(item.comments);
		}
	});
    uniq.forEach(function (item) {
        item.comments.forEach(function (comment) {
            comment.OldDocumentId = comment.DocumentId;
            comment.DocumentId = makeDocumentId(comment.DocumentId)
        });
    });
//    printjson(uniq);
    //-------------------------------------
    //  save unique 
    //-------------------------------------
	//    saveUniqueComments();
	db.Comments_Unique.drop();
    
    
    for(var i = 0; i < uniq.length;i++){
          var item = uniq[i];
          item.comments.forEach(function (com) {
           commentsOnly.push(com);
  		  });
    }
    db.Comments_Unique.insert(uniq);
    groupByDocumentId();
            

