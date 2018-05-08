function prepareForSave(comment) {
    delete comment["OldDocumentId"];
    delete comment["status"];
    comment.Replies.forEach(function (rep) {
        delete rep["moved"];
    });
}

db.CommentsForUpdate.find({}).forEach( function (item){

  if(item.status == "update"){
    prepareForSave(item);
    printjson(item);
    db.Comments.replaceOne({_id : item._id}, item);
  }
  else if(item.status == "delete"){
    prepareForSave(item);
    printjson(item);
    db.Comments.deleteOne({_id: item._id});
  }
  else{
    print(item.status);
  }
 }
);

