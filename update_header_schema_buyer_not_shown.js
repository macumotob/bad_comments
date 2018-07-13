//CompanyId: "d5710f78-9a3a-466d-b5be-2af3d44e743c"

//FieldId: "buyer_not_shown"

//1. в Мастер дате, находите филд и вытягиваете из него все Choice'ы. Это массив строк.

//2. Берете все фолдеры, в которых используется филд FieldId: "buyer_not_shown"

//3. Берете все хедеры, с фильтром по CompanyId, и FoldertType in [фолдеры из п.1]

//4. Во всех хедерах обновляете поле buyer_not_shown, заполняете в нем все Choice'ы.

//function makeValues(md){
//  var list = [];
//  for(var i =0; i < md.length; i++){
//    list.push(md[i].value);
//  }
//  var z =   md.map(function(item) {
//    return item.value;
//   });
//   print(z);
//  return list;
//}

var fieldId = "buyer_not_shown";
var companyId = "d5710f78-9a3a-466d-b5be-2af3d44e743c";

var md = db.MasterData.findOne(
    { "FieldId": fieldId, "company_id": companyId }).Properties.Choices;

//    var choices = makeValues(md);

var choices = md.map(function (item) {
    return item.value;
});

var foldersId = [];

db.Folders.find(
    { "Schema": { $elemMatch: { "FieldId": fieldId } }, "CompanyId": companyId }
).forEach(
    function (folder) {
        foldersId.push(folder._id);
    });

var count = 0;
var s;
db.Headers.find(
    {
        "CompanyId": companyId, "FolderType": { $in: foldersId },
        "Schema.buyer_not_shown": { $exists: true }
    }
).forEach(function (header) {
    if (header) {
        s = "HeaderNumber : " + header.header_number + " / " + header.Schema.buyer_not_shown.length;
        header.Schema.buyer_not_shown = choices;
        s += " : " + header.Schema.buyer_not_shown.length;
        if (count < 5) {
            //              printjson(header);
        }
        print(s);
        count++;
    }
}
);
print(count);
    //print(foldersId.length);
