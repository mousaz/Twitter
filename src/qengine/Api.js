const http = require("http");
const url = require("url");
const querystring = require("querystring");
const MongoDBClient = require("./MongoDBClient");
const fs = require("fs");
const path = require("path");
const Metrics = require("./Metrics");

const PORT = 9999;
const logger = require("../jscommon/logger").createLogger("QueryEngine:");

const dbClient = new MongoDBClient();

const server = http.createServer((req, res) => {
  logger.log(`Request received: ${req.url}`);
  const reqUrl = url.parse(req.url);

  if (reqUrl.pathname === "/Report") {
    return res.end(fs.readFileSync(path.join(__dirname, "Report.html")));
  }

  const qParams = querystring.parse(reqUrl.query);
  const matches = reqUrl.pathname.match(/^\/metrics\/(.*)\/$/);
  if (!matches || matches.length < 2) {
    res.writeHead(404);
    return res.end();
  }

  const metricName = matches[1];
  if (!Metrics[metricName]) {
    res.writeHead(404);
    return res.end();
  }

  const query = Metrics[metricName](qParams);

  dbClient
    .runQuery(query)
    .then((result) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    })
    .catch((err) => {
      logger.error(`Query execution failed with Error: ${err}`);
      res.writeHead(500);
      res.end();
    });
});

server.listen(PORT, () => {
  logger.log(`Listening on port: ${PORT}`);
});
