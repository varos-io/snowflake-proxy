const snowflake = require('./snowflake');
const express = require('express')
const process = require('process')
const app = express()
const port = 3000

app.use(express.json({
    // default is 100kb
    limit: "1000kb",
}));

app.get('/', (req, res) => {
  res.json({status: 'ok'})
});

app.post('/query', (req, res) => {
    const wharehouse = req.body.warehouse;
    const database = req.body.database;
    const query = req.body.query;
    const bindParams = req.body.bind_params;

    const runningQuery = snowflake.getPool(wharehouse, database).query(query, bindParams);
    runningQuery.fetch().then(rows => {
        res.json({
            data: rows
        });
    }).catch(x => {
        res.status(500).json({err: 'snowflake error', detail: x.toString()});
    });

    res.on("close", function() {
        // request closed unexpectedly
         if(!res.headersSent) {
            runningQuery.cancel();
         }
    });

});

app.post('/metadata', (req, res) => {
    const wharehouse = req.body.warehouse;
    const database = req.body.database;
    const query = req.body.query;
    const bindParams = req.body.bind_params;
    snowflake.getPool(wharehouse, database).statementMetadata(query, bindParams).then(rows => {
        res.json({
            data: rows
        });
    }).catch(x => {
        res.status(500).json({err: 'snowflake error', detail: x.toString()});
    });
});


app.listen(port, () => {
  console.log(`Snowflake proxy app listening at http://localhost:${port}`)
})

process.on('exit', function () {
    console.log('About to exit, cleaning up connections');
    snowflake.closePools();
});

process.on('SIGINT', () => {
  console.info("Interrupted")
  process.exit(0)
})
