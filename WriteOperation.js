'use strict'
const ba = require('ymuto-dynamodb-client');

/** @class */
module.exports = class WriteOperation {
  constructor () {
    this.params = {
      MAX_BATCH_WRITE : 25
    }
  }

  makePut (user_id, date, table_name, item) {
    let req = item;
    req.user_id     = user_id;
    req.date_create = date;
    req.date_update = date;

    let params = {
      TableName    : table_name,
      Item         : req,
      ReturnValues : "ALL_OLD"
    };

    return ba.put(params);
  }


  makeOnlyPut (table_name, item) {

    let params = {
      TableName    : table_name,
      Item         : item
    };

    return ba.put(params);
  }


  makeDel ( user_id, key, table_name ) {
    key.user_id = user_id;
    let params = {
      TableName : table_name,
      Key       : key
    };
    return ba.del(params);
  }


  makeUpdArray( user_id, date, key, table_name, updates, adds ) {
    key.user_id = user_id;
    let params = {};
    params.TableName = table_name;
    params.Key       = key;

    params.UpdateExpression = "set ";
    params.ExpressionAttributeValues = {};
    params.ExpressionAttributeNames  = {};
    params.ReturnValues = "ALL_NEW";

    Object.keys(updates).forEach((w,i)=>{

        let u = "#u" + i;
        let v = ":v" + i;
        params.ExpressionAttributeNames[u]  = w;

        params.UpdateExpression += u + "=" + v + ",";
        params.ExpressionAttributeValues[v] = updates[w];
    });


    Object.keys(adds).forEach((w,i)=>{
      if (Array.isArray(adds[w])){
        let uu = "#uu" + i;
        let vv = ":vv" + i;
        params.ExpressionAttributeNames[uu] = w;

        adds[w].forEach((x,j)=>{
          let uuv = uu + "[" + j + "]";
          params.UpdateExpression +=  uuv + "=" + uuv + "+" + vv + j + ",";

          params.ExpressionAttributeValues[vv+j] = x;
        });
      } else {
        let u = "#uu" + i;
        let v = ":vv" + i;

        params.ExpressionAttributeNames[u]  = w;

        params.UpdateExpression += u + "=" + u + "+" + v + ",";
        params.ExpressionAttributeValues[v] = adds[w];
      }
    
    });
    let uuu = "#uuu";
    let vvv = ":vvv";

    params.UpdateExpression += uuu + "=" + vvv;
    params.ExpressionAttributeNames[uuu]  = "date_update";
    params.ExpressionAttributeValues[vvv] = date;
//    console.log("params " + JSON.stringify(params));
    return ba.upd(params);
  }

  makeUpdOnly ( date, key, table_name, updates, adds ) {
    let params = {};
    params.TableName = table_name;
    params.Key       = key;

    params.UpdateExpression = "set ";
    params.ExpressionAttributeValues = {};
    params.ExpressionAttributeNames  = {};
    params.ReturnValues = "ALL_NEW";

    Object.keys(updates).forEach((w,i)=>{

        let u = "#u" + i;
        let v = ":v" + i;
        params.ExpressionAttributeNames[u]  = w;

        params.UpdateExpression += u + "=" + v + ",";
        params.ExpressionAttributeValues[v] = updates[w];

    });


    Object.keys(adds).forEach((w,i)=>{

        let u = "#uu" + i;
        let v = ":vv" + i;

        params.ExpressionAttributeNames[u]  = w;

        params.UpdateExpression += u + "=" + u + "+" + v + ",";
        params.ExpressionAttributeValues[v] = adds[w];
    });
    let uuu = "#uuu";
    let vvv = ":vvv";

    params.UpdateExpression += uuu + "=" + vvv;
    params.ExpressionAttributeNames[uuu]  = "date_update";
    params.ExpressionAttributeValues[vvv] = date;
//    console.log("params " + JSON.stringify(params));
    return ba.upd(params);
  }


  makeUpd( user_id, date, key, table_name, updates, adds ) {
    key.user_id = user_id;
    let params = {};
    params.TableName = table_name;
    params.Key       = key;

    params.UpdateExpression = "set ";
    params.ExpressionAttributeValues = {};
    params.ExpressionAttributeNames  = {};
    params.ReturnValues = "ALL_NEW";

    Object.keys(updates).forEach((w,i)=>{
      if (Array.isArray(updates[w])){
        if ( updates[w].length == 0 ){
          let u = "#u" + i;
          let v = ":v" + i;
          params.ExpressionAttributeNames[u]  = w;

          params.UpdateExpression += u + "=" + v + ",";
          params.ExpressionAttributeValues[v] = updates[w];
        }
        
        let uu = "#u" + i;
        let vv = ":v" + i;
        params.ExpressionAttributeNames[uu] = w;

        updates[w].forEach((x,j)=>{
          params.UpdateExpression +=  uu + "[" + j + "]=" + vv + j + ",";
          params.ExpressionAttributeValues[vv+j] = x;
        });
      } else {
        let u = "#u" + i;
        let v = ":v" + i;
        params.ExpressionAttributeNames[u]  = w;

        params.UpdateExpression += u + "=" + v + ",";
        params.ExpressionAttributeValues[v] = updates[w];
      }
    });


    Object.keys(adds).forEach((w,i)=>{
      if (Array.isArray(adds[w])){
        let uu = "#uu" + i;
        let vv = ":vv" + i;
        params.ExpressionAttributeNames[uu] = w;

        adds[w].forEach((x,j)=>{
          let uuv = uu + "[" + j + "]";
          params.UpdateExpression +=  uuv + "=" + uuv + "+" + vv + j + ",";

          params.ExpressionAttributeValues[vv+j] = x;
        });
      } else {
        let u = "#uu" + i;
        let v = ":vv" + i;

        params.ExpressionAttributeNames[u]  = w;

        params.UpdateExpression += u + "=" + u + "+" + v + ",";
        params.ExpressionAttributeValues[v] = adds[w];
      }
    
    });
    let uuu = "#uuu";
    let vvv = ":vvv";

    params.UpdateExpression += uuu + "=" + vvv;
    params.ExpressionAttributeNames[uuu]  = "date_update";
    params.ExpressionAttributeValues[vvv] = date;
//    console.log("params " + JSON.stringify(params));
    return ba.upd(params);
  }

  makeUpds( user_id, date, key_s, table_name, updates_s ) {
    let params_s = [];
    key_s.forEach((key,i)=>{
//      console.log(JSON.stringify(key));
      let updates = updates_s[i].updates;

      key.user_id = user_id;
      let params = {};
      params.TableName = table_name;
      params.Key       = key;

      params.UpdateExpression = "set ";
      params.ExpressionAttributeValues = {};
      params.ExpressionAttributeNames  = {};
      params.ReturnValues = "ALL_NEW";

      Object.keys(updates).forEach((w,i)=>{
        if (Array.isArray(updates[w])){
          let uu = "#u" + i;
          let vv = ":v" + i;
          params.ExpressionAttributeNames[uu] = w;

          update[w].forEach((x,j)=>{
            params.UpdateExpression +=  uu + "[" + j + "]=" + vv + j + ",";
            params.ExpressionAttributeValues[vv+j] = x;
          });
        } else {
          let u = "#u" + i;
          let v = ":v" + i;
          params.ExpressionAttributeNames[u]  = w;
          params.UpdateExpression += u + "=" + v + ",";
          params.ExpressionAttributeValues[v] = updates[w];
        }
      });
      let uuu = "#uuu";
      let vvv = ":vvv";

      params.UpdateExpression += uuu + "=" + vvv;
      params.ExpressionAttributeNames[uuu]  = "date_update";
      params.ExpressionAttributeValues[vvv] = date;
//      console.log("params " + JSON.stringify(params));
      params_s.push(params);
    });
    return ba.upds(params_s);
  }


  makeUpdsAdds( user_id, date, key_s, table_name, items_s ) {
    let params_s = [];
    key_s.forEach((key,i)=>{
//      console.log(JSON.stringify(key));
      let updates = items_s[i].updates;
      let adds    = items_s[i].adds;      

      key.user_id = user_id;
      let params = {};
      params.TableName = table_name;
      params.Key       = key;

      params.UpdateExpression = "set ";
      params.ExpressionAttributeValues = {};
      params.ExpressionAttributeNames  = {};
      params.ReturnValues = "ALL_NEW";

      if ( updates ) {
      Object.keys(updates).forEach((w,i)=>{
        if (Array.isArray(updates[w])){
          let uu = "#u" + i;
          let vv = ":v" + i;
          params.ExpressionAttributeNames[uu] = w;

          update[w].forEach((x,j)=>{
            params.UpdateExpression +=  uu + "[" + j + "]=" + vv + j + ",";
            params.ExpressionAttributeValues[vv+j] = x;
          });
        } else {
          let u = "#u" + i;
          let v = ":v" + i;
          params.ExpressionAttributeNames[u]  = w;

          params.UpdateExpression += u + "=" + v + ",";
          params.ExpressionAttributeValues[v] = updates[w];
        }
      });
      }

      if ( adds ) {
      Object.keys(adds).forEach((w,i)=>{
        if (Array.isArray(adds[w])){
          let uu = "#uu" + i;
          let vv = ":vv" + i;
          params.ExpressionAttributeNames[uu] = w;

          adds[w].forEach((x,j)=>{
            let uuv = uu + "[" + j + "]";
            params.UpdateExpression +=  uuv + "=" + uuv + "+" + vv + j + ",";

            params.ExpressionAttributeValues[vv+j] = x;
          });
        } else {
          let u = "#uu" + i;
          let v = ":vv" + i;

          params.ExpressionAttributeNames[u]  = w;
          params.UpdateExpression += u + "=" + u + "+" + v + ",";
          params.ExpressionAttributeValues[v] = adds[w];
        }
      });
      }

      let uuu = "#uuu";
      let vvv = ":vvv";

      params.UpdateExpression += uuu + "=" + vvv;
      params.ExpressionAttributeNames[uuu]  = "date_update";
      params.ExpressionAttributeValues[vvv] = date;
//      console.log("params " + JSON.stringify(params));
      params_s.push(params);
    });
    return ba.upds(params_s);
  }

  makeBatchWriteMultiTable (user_id, date, table_name_s, items_s) {
    let length = 0;
    items_s.forEach((v,i)=>{ length += v.length; });
    if (length > this.params.MAX_BATCH_WRITE)
    {
      throw new Error("makeBatchWriteMultiTable items length > 25");
    }
    
    let itemss = [];
    items_s.forEach((items,i)=>{
      itemss.push(this._addUserIdAndDateToItem(user_id, date, items));
    });
    let params = this._returnBatchWriteMultiTables(table_name_s, itemss);
//    console.log(JSON.stringify(params));
    return ba.btw(params);
  }

  makeBatchWrites (user_id, date, table_name, items) {

    let items_s = this._makeWriteRequestItems(user_id, date, table_name, items);

    let params_s = [];
    for ( let i = 0; i < items_s.length; i ++)
    {
      let params = this._returnBatchWriteMultiTables([table_name], [items_s[i]]);
      params_s.push(params);
    }
    return ba.btws(params_s);
  }

  // 複数のテーブルとそれぞれに対応する複数のアイテムに対応
  // table_name_s = ["users",   "users_status", "users_characters"]
  // items_s      = [[{item1}], [{item1}],      [{item1},{item2}] ]
  _returnBatchWriteMultiTables ( table_name_s, items_s )
  {
    let requestItems = {};
    table_name_s.forEach((tableName,i)=>{
      requestItems[tableName] = this._returnBatchWriteRequestItems(items_s[i]);
    });
    let params = {
      RequestItems : requestItems
    };
    return params;
  }

  _returnBatchDeleteMultiTables ( table_name, items )
  {
    let requestItems = {};
    requestItems[table_name] = this._returnBatchDeleteRequestItems(items);

    let params = {
      RequestItems : requestItems
    };
    return params;
  }


  makeBatchDeletes ( user_id, table_name, items )
  {
    let items_s = this._makeDeleteRequestItems(user_id, items);

    let params_s = [];
    for ( let i = 0; i < items_s.length; i ++)
    {
      let params = this._returnBatchDeleteMultiTables(table_name, items_s[i]);
      params_s.push(params);
    }
    return ba.btws(params_s);
  }


  _returnBatchDeleteRequestItems ( items )
  {
    let deleteRequests = [];
    items.forEach((item,i)=>{
      let deleteRequest = {
        DeleteRequest : {
          Key : item
        }
      };
      deleteRequests.push(deleteRequest);
    });
    return deleteRequests;
  }


  _returnBatchWriteRequestItems ( items )
  {
    let putRequests = [];
    items.forEach((item,i)=>{
      let putRequest = {
        PutRequest : {
          Item : item
        }
      };
      putRequests.push(putRequest);
    });
    return putRequests;
  }

  _makeWriteRequestItems ( user_id, date, table_name, items )
  {
    let itemLength = Math.ceil(items.length/this.params.MAX_BATCH_WRITE);
    let items_s = [];

    for ( let i = 0; i < itemLength - 1; i++ )
    {
      let eachItems = [];
      for ( let j = 0; j < this.params.MAX_BATCH_WRITE; j++ )
      {
        eachItems.push(items[j+i*this.params.MAX_BATCH_WRITE]);
      }
      items_s.push(this._addUserIdAndDateToItem(user_id, date, eachItems));
    }

    let eachItems = [];
    for ( let j = (itemLength-1)*this.params.MAX_BATCH_WRITE ; j < items.length ; j++ )
    {
      eachItems.push(items[j]);
    }

//    console.log(JSON.stringify(itemLength));
//    console.log(JSON.stringify(eachItems));
    items_s.push(this._addUserIdAndDateToItem(user_id, date, eachItems));
    return items_s;
  }


  _makeDeleteRequestItems ( user_id, items )
  {
    let itemLength = Math.ceil(items.length/this.params.MAX_BATCH_WRITE);
    let items_s = [];

    for ( let i = 0; i < itemLength - 1; i++ )
    {
      let eachItems = [];
      for ( let j = 0; j < this.params.MAX_BATCH_WRITE; j++ )
      {
        eachItems.push(items[j+i*this.params.MAX_BATCH_WRITE]);
      }
      items_s.push(this._addUserIdToItem(user_id, eachItems));
    }

    let eachItems = [];
    for ( let j = (itemLength-1)*this.params.MAX_BATCH_WRITE ; j < items.length ; j++ )
    {
      eachItems.push(items[j]);
    }
//    console.log(JSON.stringify(itemLength));
//    console.log(JSON.stringify(eachItems));
    items_s.push(this._addUserIdToItem(user_id, eachItems));
    return items_s;
  }

  _addUserIdToItem ( user_id, items)
  {
    items.forEach((v,i)=>{
      v.user_id = user_id;
    });
    return items;
  }


  _addUserIdAndDateToItem ( user_id, date, items )
  {
    items.forEach((v,i)=>{
      v.user_id = user_id;
      v.date_create = date;
      v.date_update = date;
    })
    return items;
  }



  makeBatchWritesNotUserId ( date, table_name, items) {

    let items_s = this._makeWriteRequestItemsDate( date, table_name, items);

    let params_s = [];
    for ( let i = 0; i < items_s.length; i ++)
    {
      let params = this._returnBatchWriteMultiTables([table_name], [items_s[i]]);
      params_s.push(params);
    }
    return ba.btws(params_s);
  }



  _makeWriteRequestItemsDate ( date, table_name, items )
  {
    let itemLength = Math.ceil(items.length/this.params.MAX_BATCH_WRITE);
    let items_s = [];

    for ( let i = 0; i < itemLength - 1; i++ )
    {
      let eachItems = [];
      for ( let j = 0; j < this.params.MAX_BATCH_WRITE; j++ )
      {
        eachItems.push(items[j+i*this.params.MAX_BATCH_WRITE]);
      }
      items_s.push(this._addDateToItem(date, eachItems));
    }

    let eachItems = [];
    for ( let j = (itemLength-1)*this.params.MAX_BATCH_WRITE ; j < items.length ; j++ )
    {
      eachItems.push(items[j]);
    }

//    console.log(JSON.stringify(itemLength));
//    console.log(JSON.stringify(eachItems));
    items_s.push(this._addDateToItem(date, eachItems));
    return items_s;
  }


  _addDateToItem ( date, items )
  {
    items.forEach((v,i)=>{
      v.date_create = date;
      v.date_update = date;
    })
    return items;
  }

};