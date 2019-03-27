var util = require('util'),
  Store = require('../base'),
  _ = require('lodash'),
  debug = require('debug')('saga:dynamodb'),
  ConcurrencyError = require('../../errors/concurrencyError'),  
  aws = require('aws-sdk'), 
  uuid = require('uuid').v4;

//https://medium.com/quick-code/node-js-restful-api-with-dynamodb-local-7e342a934a24
function DynamoDB(options) {
  Store.call(this, options);

  var awsConf = {
    endpointConf: {}
  };

  if (process.env['AWS_DYNAMODB_ENDPOINT']) {
    awsConf.endpointConf = { endpoint: process.env['AWS_DYNAMODB_ENDPOINT'] };
  }

  var defaults = {
    s: 'sagas',
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 3
  };

  _.defaults(options, defaults);

  this.options = options;
}

util.inherits(DynamoDB, Store);

_.extend(DynamoDB.prototype, {

  connect: function(callback) {
    var self = this;
    self.client = new aws.DynamoDB(self.options.endpointConf);
    self.documentClient = new aws.DynamoDB.DocumentClient({ service: self.client });
    self.isConnected = true;

    var createSagaTable = function(done) {
      createTableIfNotExists(
        self.client,
        SagaTableDefinition(self.options),
        done
      );
    };

    createSagaTable(function(err) {
      if (err) {
        if (callback) callback(err);
      } else {
        self.emit('connect');
        if (callback) callback(null, self);
      }
    });
  },

  disconnect: function(callback) {
    this.emit('disconnect');
    if (callback) callback(null);
  },

  save: function (saga, cmds, callback) {
    if (!saga || !_.isObject(saga) || !_.isString(saga.id) || !_.isDate(saga._commitStamp)) {
      var err = new Error('Please pass a valid saga!');
      debug(err);
      if (callback) callback(err);
      return;
    }

    if (!cmds || !_.isArray(cmds)) {
      var err = new Error('Please pass a valid saga!');
      debug(err);
      if (callback) callback(err);
      return;
    }

    if (cmds.length > 0) {
      for (var c in cmds) {
        var cmd = cmds[c];
        if (!cmd.id || !_.isString(cmd.id) || !cmd.payload) {
          var err = new Error('Please pass a valid commands array!');
          debug(err);
          if (callback) callback(err);
          return;
        }
      }
    }

    if (cmds) {
      saga._commands = cmds.reduce((result, cmd) => {
        result[cmd.id] = cmd;
        return result;
      } , {});
    }
    

    if (saga._timeoutAt) {
      saga._timeoutAt = sage._timeoutAt.getTime();
      saga._timeoutAtYearMonth = new Date().toISOString().slice(0, 7).replace("-", "");
    } 

    if (saga._commitStamp) {
      saga._commitStampYearMonth = new Date().toISOString().slice(0, 7).replace("-", "");
    }

    if (!saga._hash) {
      saga._hash = uuid().toString();
      
      var params = {
        TableName: self.options.sagaTableName,
        Item: saga,
        ConditionExpression: "#id <> :id",
        ExpressionAttributeNames: { 
          "#id" : "id" 
        },
        ExpressionAttributeValues: {
          ":id": saga.id
        }
      };
      self.documentClient.put(params, function(err, data) {
        if (err) debug(err);

        if (err && err.code && err.code >= "ConditionalCheckFailedException") {
          if (callback) callback(new ConcurrencyError());
          return;
        }
        if (callback) callback(err);
      });
    } else {
      var currentHash = saga._hash;
      saga._hash = new ObjectID().toString();

      var params = {
        TableName: self.options.sagaTableName,
        Item: saga,
        ConditionExpression: "#id = :id AND #hash = :_hash",
        ExpressionAttributeNames: { 
          "#id" : "id",
          "#hash" : "_hash"
        },
        ExpressionAttributeValues: {
          ":id": saga.id,
          ":_hash": currentHash
        }
      };
      self.documentClient.put(params, function(err, data) {
        if (err) debug(err);

        if (err && err.code && err.code >= "ConditionalCheckFailedException") {
          if (callback) callback(new ConcurrencyError());
          return;
        }

        if (callback) callback(err);
      });
    }
  },

  get: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      debug(err);
      if (callback) callback(err);
      return;
    }

    var params = {
      TableName: self.options.sagaTableName,
      Key:{
        "id": id
      }
    };
    self.documentClient.get(params, function(err, data) {
      if (err) {
        if (callback) callback(err);
        return;
      } 

      var saga = data.item;
      if (saga === undefined) {
        if (callback) callback(null, null);
        return;
      }

      if (saga._commands) {
        delete saga._commands;
      }

      if (saga._commitStampYearMonth) {
        delete saga._commitStampYearMonth;
      }

      if (saga._timeoutAtYearMonth) {
        delete saga._timeoutAtYearMonth;
      }

      if (saga._timeoutAt) {
        saga._timeoutAt = new Date(parseInt(saga._timeoutAt));
      }
      
      if (saga._commitStamp) {
        saga._commitStamp = new Date(parseInt(saga._commitStamp));
      }

      if (callback) callback(null, saga);
    });
  },

  remove: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      debug(err);
      if (callback) callback(err);
      return;
    }

    var params = {
      TableName: self.options.sagaTableName,
      Key:{
        "id": id
      }
    };
    self.documentClient.get(params, function(err, data) {
      if (callback) callback(err);
      return;
    });
  },

  getTimeoutedSagas: function (options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }
    options = options || {};

    var params = {
      TableName : self.options.sagaTableName,
      IndexName : '_timeoutAt_index',
      FilterExpression: "#timeoutAt <= :_timeoutAt",
      "ExpressionAttributeNames": { 
        "#timeoutAt" : "_timeoutAt" 
      },
      ExpressionAttributeValues : {
        ":_timeoutAt": new Date().getTime()
      }
    };

    self.documentClient.scan(params, function(err, data) {
      if (err) {
        debug(err);
        if (callback) callback(err);
        return;
      } 

      var sagas = data.Items;
      if (sagas === undefined) {
        if (callback) callback(null, null);
        return;
      }

      sagas.forEach(function (s) {
        if (s._timeoutAt) {
          s._timeoutAt = new Date(parseInt(s._timeoutAt));
        }
        
        if (s._commitStamp) {
          s._commitStamp = new Date(parseInt(s._commitStamp));
        }

        if (s._commands) {
          delete s._commands;
        }

        if (s._commitStampYearMonth) {
          delete s._commitStampYearMonth;
        }
  
        if (s._timeoutAtYearMonth) {
          delete s._timeoutAtYearMonth;
        }
      });

      if (callback) callback(null, sagas);
    });
  },

  getOlderSagas: function (date, callback) {
    if (!date || !_.isDate(date)) {
      var err = new Error('Please pass a valid date object!');
      debug(err);
      if (callback) callback(err);
      return;
    }

    var params = {
      TableName : self.options.sagaTableName,
      IndexName : '_commitStamp_index',
      FilterExpression: "#commitStamp <= :_commitStamp",
      "ExpressionAttributeNames": { 
        "#commitStamp" : "_commitStamp" 
      },
      ExpressionAttributeValues : {
        ":_commitStamp": date.getTime()
      }
    };

    self.documentClient.scan(params, function(err, data) {
      if (err) {
        if (callback) callback(err);
        return;
      } 

      var sagas = data.Items;
      if (sagas === undefined) {
        if (callback) callback(null, null);
        return;
      }

      sagas.forEach(function (s) {
        if (s._timeoutAt) {
          s._timeoutAt = new Date(parseInt(s._timeoutAt));
        }
        
        if (s._commitStamp) {
          s._commitStamp = new Date(parseInt(s._commitStamp));
        }

        if (s._commands) {
          delete s._commands;
        }

        if (s._commitStampYearMonth) {
          delete s._commitStampYearMonth;
        }
  
        if (s._timeoutAtYearMonth) {
          delete s._timeoutAtYearMonth;
        }
      });

      if (callback) callback(null, sagas);
    });
  },

  getUndispatchedCommands: function (options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }
    options = options || {};

    var res = [];

    var params = {
      TableName : self.options.sagaTableName,
      IndexName : '_commitStamp_index',
      FilterExpression: "attribute_exists(#commands)",
      "ExpressionAttributeNames": { 
        "#commands" : "_commands" 
      }
    };

    self.documentClient.scan(params, function(err, data) {
      if (err) {
        debug(err);
        if (callback) callback(err);
        return;
      } 

      var sagas = data.Items;
      if (sagas === undefined) {
        if (callback) callback(null, null);
        return;
      }

      sagas.forEach(function (s) {
        if (s._timeoutAt) {
          s._timeoutAt = new Date(parseInt(s._timeoutAt));
        }
        
        if (s._commitStamp) {
          s._commitStamp = new Date(parseInt(s._commitStamp));
        }
        
        if (s._commands) {
          if (s._commands.length > 0) {
            Object.values(s._commands).forEach(function (c) {
              res.push({ sagaId: s._id, commandId: c.id, command: c.payload, commitStamp: s._commitStamp });
            });
          }

          delete s._commands;
        }

        if (s._commitStampYearMonth) {
          delete s._commitStampYearMonth;
        }
  
        if (s._timeoutAtYearMonth) {
          delete s._timeoutAtYearMonth;
        }
      });

      if (callback) callback(null, res);
    });
  },

  setCommandToDispatched: function (cmdId, sagaId, callback) {
    if (!cmdId || !_.isString(cmdId)) {
      var err = new Error('Please pass a valid command id!');
      debug(err);
      if (callback) callback(err);
      return;
    }

    if (!sagaId || !_.isString(sagaId)) {
      var err = new Error('Please pass a valid saga id!');
      debug(err);
      if (callback) callback(err);
      return;
    }

    var params = {
      TableName : self.options.sagaTableName,
      Key : {
        id: sagaId
      },
      UpdateExpression: "REMOVE #commands.#cmdId",
      ExpressionAttributeNames: {
        "#commands": "_commands",
        "#cmdId": cmdId 
      }
    };

    self.documentClient.update(params, function(err, data) {
      if (callback) {
        callback(err);
      }
    });
  },

  clear: function (callback) {
    console.error("this function is not implemented");
    callback(null);
  }

});

function SagaTableDefinition(opts) {
  var def = {
    TableName: opts.sagaTableName,
    KeySchema: [
      { AttributeName: 'id', KeyType: 'HASH' }
    ],
    AttributeDefinitions: [
      { AttributeName: 'id', AttributeType: 'S' },
      { AttributeName: '_commitStampYearMonth', AttributeType: 'S' },
      { AttributeName: '_commitStamp', AttributeType: 'N' },
      { AttributeName: '_timeoutAtYearMonth', AttributeType: 'S' },
      { AttributeName: '_timeoutAt', AttributeType: 'N' }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: opts.primaryReadCapacityUnits || 5,
      WriteCapacityUnits: opts.primaryWriteCapacityUnits || 5
    }
  };

  var globalSecondaryIndexes = [ // optional (list of GlobalSecondaryIndex)
    { 
      IndexName: '_commitStamp_index', 
      KeySchema: [
        { AttributeName: '_commitStampYearMonth', KeyType: 'HASH' },
        { 
          AttributeName: '_commitStamp',
          KeyType: 'RANGE',
        }
      ],
      Projection: {
        ProjectionType: 'ALL' // (ALL | KEYS_ONLY | INCLUDE)
      },
      ProvisionedThroughput: { // throughput to provision to the index
        ReadCapacityUnits: opts.commitStampReadCapacityUnits || 5,
        WriteCapacityUnits: opts.commitStampWriteCapacityUnits || 5
      },
    },
    { 
      IndexName: '_timeoutAt_index', 
      KeySchema: [
        { AttributeName: '_timeoutAtYearMonth', KeyType: 'HASH' },
        { 
          AttributeName: '_timeoutAt',
          KeyType: 'RANGE',
        }
      ],
      Projection: {
        ProjectionType: 'ALL' // (ALL | KEYS_ONLY | INCLUDE)
      },
      ProvisionedThroughput: { // throughput to provision to the index
        ReadCapacityUnits: opts.timeoutAtReadCapacityUnits || 5,
        WriteCapacityUnits: opts.timeoutAtWriteCapacityUnits || 5
      },
    }
  ]
  
  def.GlobalSecondaryIndexes = globalSecondaryIndexes;

  return def;
}

var createTableIfNotExists = function(client, params, callback) {
  var exists = function(p, cbExists) {
    client.describeTable({ TableName: p.TableName }, function(err, data) {
      if (err) {
        if (err.code === 'ResourceNotFoundException') {
          cbExists(null, { exists: false, definition: p });
        } else {
          cbExists(err);
        }
      } else {
        cbExists(null, { exists: true, description: data });
      }
    });
  };

  var create = function(r, cbCreate) {
    if (!r.exists) {
      client.createTable(r.definition, function(err, data) {
        if (err) {
          cbCreate(err);
        } else {
          cbCreate(null, {
            Table: {
              TableName: data.TableDescription.TableName,
              TableStatus: data.TableDescription.TableStatus
            }
          });
        }
      });
    } else {
      cbCreate(null, r.description);
    }
  };

  var active = function(d, cbActive) {
    var status = d.Table.TableStatus;
    async.until(
      function() {
        return status === 'ACTIVE';
      },
      function(cbUntil) {
        client.describeTable({ TableName: d.Table.TableName }, function(
          err,
          data
        ) {
          if (err) {
            cbUntil(err);
          } else {
            status = data.Table.TableStatus;
            setTimeout(cbUntil, 1000);
          }
        });
      },
      function(err, r) {
        if (err) {
          return cbActive(err);
        }
        cbActive(null, r);
      }
    );
  };

  async.compose(active, create, exists)(params, function(err, result) {
    if (err) callback(err);
    else callback(null, result);
  });
};

module.exports = DynamoDB;