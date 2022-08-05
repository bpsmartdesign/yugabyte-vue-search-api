var express = require("express");
const redis = require("redis");
const yugabyte = require("cassandra-driver");
var _ = require("underscore");
var fs = require("fs");

var router = express.Router();

var configPath = "./config.json";
var table = "yugabyte-search-product.products"
var options = JSON.parse(fs.readFileSync(configPath, "UTF-8"));

DB_HOST = options.DB_HOST;
console.log("DB host: " + options.DB_HOST);

const ybRedis = redis.createClient({ host: DB_HOST });
const ybCassandra = new yugabyte.Client({
  contactPoints: [DB_HOST],
  keyspace: "yugabyte-search-product",
});

ybCassandra.connect(function (err) {
  if (err) {
    console.log(err);
  }
});

/* List all products. */
router.get("/", function (req, res, next) {
  productListing = [];
  ybCassandra.execute(`SELECT * FROM ${table};`).then((result) => {
    const row = result.first();
    for (var i = 0; i < result.rows.length; i++) {
      productListing.push(result.rows[i]);
    }
    return res.json(productListing);
  });
});

/* Return details of a specific product id. */
router.get("/:id", function (req, res, next) {
  var productDetails = {};
  var selectStmt = `SELECT * FROM ${table} WHERE id=${req.params.id};`;
  ybCassandra.execute(selectStmt).then((result) => {
    var row = result.first();
    productDetails = Object.assign({}, row);
    return res.json(productDetails);
  });
});

module.exports = router;
