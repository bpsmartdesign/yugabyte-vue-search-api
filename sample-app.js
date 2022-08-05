var pg = require("pg");
const async = require("async");
const fs = require("fs");
const { callbackify } = require("util");
const { rows } = require("pg/lib/defaults");

const config = {
  host: "us-west-2.1dc12fd2-4161-4e46-b889-465dcefbfabb.aws.ybdb.io:5433/",
  port: "5433",
  database: "yugabyte",
  user: "admin",
  password: "nAJcJGpes48JX9pN40PJpLlKKn7tEV",
  // ssl: {
  //     rejectUnauthorized: true,
  //     ca: fs.readFileSync('path_to_your_root_certificate').toString()
  // },
  connectionTimeoutMillis: 5000,
};

var client;

async function connect(callbackHandler) {
  console.log(">>>> Connecting to YugabyteDB!");

  try {
    client = new pg.Client(config);

    await client.connect();

    console.log(">>>> Connected to YugabyteDB!");

    callbackHandler();
  } catch (err) {
    callbackHandler(err);
  }
}

async function createDatabase(callbackHandler) {
  try {
    var stmt = "DROP TABLE IF EXISTS DemoAccount";

    await client.query(stmt);

    stmt = `CREATE TABLE DemoAccount (
            id int PRIMARY KEY,
            name varchar,
            age int,
            country varchar,
            balance int)`;

    await client.query(stmt);

    stmt = `INSERT INTO DemoAccount VALUES
            (1, 'Jessica', 28, 'USA', 10000),
            (2, 'John', 28, 'Canada', 9000)`;

    await client.query(stmt);

    console.log(">>>> Successfully created table DemoAccount.");

    callbackHandler();
  } catch (err) {
    callbackHandler(err);
  }
}

async function selectAccounts(callbackHandler) {
  console.log(">>>> Selecting accounts:");

  try {
    const res = await client.query(
      "SELECT name, age, country, balance FROM DemoAccount"
    );
    var row;

    for (i = 0; i < res.rows.length; i++) {
      row = res.rows[i];

      console.log(
        "name = %s, age = %d, country = %s, balance = %d",
        row.name,
        row.age,
        row.country,
        row.balance
      );
    }

    callbackHandler();
  } catch (err) {
    callbackHandler(err);
  }
}

async function transferMoneyBetweenAccounts(callbackHandler, amount) {
  try {
    await client.query("BEGIN TRANSACTION");

    await client.query(
      "UPDATE DemoAccount SET balance = balance - " +
        amount +
        " WHERE name = 'Jessica'"
    );
    await client.query(
      "UPDATE DemoAccount SET balance = balance + " +
        amount +
        " WHERE name = 'John'"
    );
    await client.query("COMMIT");

    console.log(">>>> Transferred %d between accounts.", amount);

    callbackHandler();
  } catch (err) {
    callbackHandler(err);
  }
}

async.series(
  [
    function (callbackHandler) {
      connect(callbackHandler);
    },
    function (callbackHandler) {
      createDatabase(callbackHandler);
    },
    function (callbackHandler) {
      selectAccounts(callbackHandler);
    },
    function (callbackHandler) {
      transferMoneyBetweenAccounts(callbackHandler, 800);
    },
    function (callbackHandler) {
      selectAccounts(callbackHandler);
    },
  ],
  function (err) {
    if (err) {
      // Applies to logic of the transferMoneyBetweenAccounts method
      if (err.code == 40001) {
        console.error(
          `The operation is aborted due to a concurrent transaction that is modifying the same set of rows. Consider adding retry logic or using the pessimistic locking.`
        );
      }

      console.error(err);
    }
    client.end();
  }
);
