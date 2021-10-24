const snowflake = require('snowflake-sdk');
const genericPool = require("generic-pool");

class PoolFactory {

    constructor(warehouse, database) {
        this.warehouse = warehouse;
        this.database = database;

    }
    create = () => {
        return new Promise((resolve, reject) => {
            // Create Connection
            const connection = snowflake.createConnection({
                account: process.env.SF_ACCOUNT,
                username: process.env.SF_USER,
                password: process.env.SF_PASSWORD,
                warehouse: this.warehouse,//process.env.SF_WAREHOUSE,
                database: this.database//process.env.SF_DATABASE
            });
            // Try to connect to Snowflake, and check whether the connection was successful.
            connection.connect((err, conn) => {
                if (err) {
                    console.error('Unable to connect: ' + err.message);
                    reject(new Error(err.message));
                } else {
                    console.log('Successfully connected to Snowflake, ID:', conn.getId());
                    resolve(conn);
                }
            });
            connection.execute({ sqlText: ""})
        });
    }
    destroy = (connection) => {
        return new Promise((resolve, reject) => {
            connection.destroy((err, conn) => {
                if (err) {
                    console.error('Unable to disconnect: ' + err.message);
                } else {
                    console.log('Disconnected connection with id: ' + conn.getId());
                }
                resolve(); // Always resolve for destroy
            });
        });
    }
    validate = (connection) => {
        return new Promise((resolve, reject) => {
            resolve(connection.isUp());
        });
    }
}

const opts = {
    max: 12, // Maximum size of the pool
    min: 0, // Minimum size of the pool,
    testOnBorrow: true, // Validate connection before acquiring it
    acquireTimeoutMillis: 60000, // Timeout to acquire connection
    evictionRunIntervalMillis: 30000, // Check every 15 min for ideal connection
    numTestsPerEvictionRun: 4, // Check only 4 connections every 15 min
    idleTimeoutMillis: 60000 // Evict only if connection is ideal for 3 hrs
};


class SnowFlakePool {
    constructor(warehouse, database) {
        this.myPool = genericPool.createPool(new PoolFactory(warehouse, database), opts);

        this.myPool.on('factoryCreateError', function(err){
            //log stuff maybe
            console.log('factoryCreateError', err);
          })
          
          this.myPool.on('factoryDestroyError', function(err){
            //log stuff maybe
            console.log('factoryDestroyError', err);
        });
    }

    statementMetadata = (sqlText, bindParams = []) => {
        return new Promise((resolve, reject) => {
            this.myPool.acquire().then(connection => {
                const stmt = connection.execute({
                    sqlText,
                    bindParams,
                    streamResult: true,
                    complete: (err, stmt, rows) => {
                        err ? reject(new Error(err.message)) : (
                        resolve({
                            numRows: stmt.getNumRows(), // $ExpectType number
                            updatedRows: stmt.getNumUpdatedRows(), // $ExpectType number
                            reqId: stmt.getRequestId(), // $ExpectType string
                            stmtId: stmt.getStatementId(), // $ExpectType string
                            cols: stmt.getColumns().map(c => ({
                                name: c.getName(),
                                index: c.getIndex(),
                                type: c.getType()
                            }))
                        }));
                        // Return connection back to pool
                        this.myPool.release(connection);
                    }
                });
                
            })
        });
    }

    query = (sqlText, bindParams = []) => {
        return new Promise((resolve, reject) => {
            // Acquire connection from pool
            this.myPool.acquire().then(connection => {
                // Execute the query
                connection.execute({
                    sqlText,
                    binds: bindParams,
                    complete: (err, stmt, rows) => {
                        console.log(`Conn: ${connection.getId()} fetched ${rows && rows.length} rows`);
                        // Return result
                        if (err) {
                            console.error(sqlText, bindParams, err)
                            reject(new Error(err.message))
                            return;
                        }
                        // err ? reject(new Error(err.message)) : resolve(rows);
    
                        var stream = stmt.streamRows();
                        
                        stream.on('error', function(err1) {
                            console.error('Unable to consume all rows');
                            reject(new Error(err1.toString()));
                        });
                        const data = []
                        stream.on('data', function(row) {
                            const columns = stmt.getColumns();
                            const rowData = new Array(columns.length);
                            columns.map(c => {
                                rowData[c.getIndex()] = row[c.getName()];
                            })
                            data.push(rowData);
                            // consume result row...
                        });
                        
                        stream.on('end', function() {
                            console.log('All rows consumed');
                            resolve(data);
                        }); 
    
                        // Return connection back to pool
                        this.myPool.release(connection);
                    }
                });
            }).catch(err => reject(new Error(err.message)));
        });
    }

    shutdownPool = () => {
        return this.myPool.drain().then(function() {
            return this.myPool.clear();
        })
    }
}
// const myPool = genericPool.createPool(factory, opts);
// myPool.on('factoryCreateError', function(err){
//     //log stuff maybe
//     console.log('factoryCreateError', err);
//   })
  
// myPool.on('factoryDestroyError', function(err){
//     //log stuff maybe
//     console.log('factoryDestroyError', err);
// });


// function shutdownPool() {
//     myPool.drain().then(function() {
//         return pool.clear();
//     })
// }

const pools = new Map();
function getPool(wharehouse, database) {
    const key = `${wharehouse}.${database}`;
    let p = pools.get(key);
    if(!p) {
        p = new SnowFlakePool(wharehouse, database);
        pools.set(key, p); 
    }
    return p;
}


function closePools() {
    return Promise.all(Object.values(pools).map(v => v.shutdownPool()));
}

module.exports = { getPool, closePools };
