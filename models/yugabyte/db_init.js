const pg = require("pg");
const async = require("async");
const sample_data = require("../sample_data.json");

const fs = require("fs");

const configPath = "./config.json";
const certPath = "./root.crt";
const options = JSON.parse(fs.readFileSync(configPath, "UTF-8"));
const config = {
  host: options.DB_HOST,
  port: options.DB_PORT,
  database: options.DB_NAME,
  user: options.DB_USER,
  password: options.DB_PASSWORD,
  connectionTimeoutMillis: options.DB_CONNECTION_LIMIT,
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync(certPath),
  },
};

let client;
const connect = async (callbackHandler) => {
  console.log(">>>> Connecting to YugabyteDB!");
  try {
    client = new pg.Client(config);
    await client.connect();

    console.log(">>>> Connected to YugabyteDB!");
    callbackHandler();
  } catch (err) {
    callbackHandler(err);
  }
};

//#region tables creation.
async function createProductsTable(callbackHandler) {
  try {
    const create_table = `CREATE TABLE IF NOT EXISTS products ( 
      id int PRIMARY KEY,
      categoryId int,
      supplierId int,
      unitStock TEXT,
      unitOrder TEXT,
      unitPrice int,
      reorderLevel int,
      discontinued boolean,
      quantity int,
      name TEXT,
      description TEXT,
      author TEXT,
      type TEXT,
      img TEXT)`;
  
    await client.query(create_table);
    await loadProducts();

    console.log(">>>> Successfully created table products.");
    callbackHandler();
  } catch (err) {
    callbackHandler(err);
  }
}
async function createCategoriesTable(callbackHandler) {
  try {
    const create_table = `CREATE TABLE IF NOT EXISTS categories ( 
      id int PRIMARY KEY,
      name TEXT,
      description TEXT)`;
  
    await client.query(create_table);
    await loadCategories();

    console.log(">>>> Successfully created table products.");
    callbackHandler();
  } catch (err) {
    callbackHandler(err);
  }
}
async function createSuppliersTable(callbackHandler) {
  try {
    const create_table = `CREATE TABLE IF NOT EXISTS suppliers ( 
      id int PRIMARY KEY,
      name TEXT,
      img TEXT)`;
  
    await client.query(create_table);
    await loadSuppliers();

    console.log(">>>> Successfully created table products.");
    callbackHandler();
  } catch (err) {
    callbackHandler(err);
  }
}
//#endregion

//#region insert dummy data to the database
async function loadProducts() {
  const dbValues = sample_data.products.reduce((p, c) => {
    p += `(${c.id}, ${c.categoryId}, ${c.supplierId}, ${c.unitStock}, ${c.unitOrder}, ${c.unitPrice}, ${c.reorderLevel}, ${c.discontinued}, ${c.quantity}, ${c.name}, ${c.description}, ${c.author}, ${c.type}, ${c.img}),`;
    return p;
  }, "");
  const insert = `INSERT INTO products VALUES ${dbValues}`;
  await client.query(insert)
}
async function loadCategories() {
  const dbValues = sample_data.categories.reduce((p, c) => {
    p += `(${c.id} ${c.name}, ${c.description}),`;
    return p;
  }, "");
  const insert = `INSERT INTO categories VALUES ${dbValues}`;
  await client.query(insert)
}
async function loadSuppliers() {
  const dbValues = sample_data.suppliers.reduce((p, c) => {
    p += `(${c.id}, ${c.name}, ${c.img}),`;
    return p;
  }, "");
  const insert = `INSERT INTO suppliers VALUES ${dbValues}`;
  await client.query(insert)
}
//#endregion

async.series(
  [
    function (callbackHandler) {
      connect(callbackHandler);
    },
    function (callbackHandler) {
      createProductsTable(callbackHandler);
    },
    function (callbackHandler) {
      createCategoriesTable(callbackHandler);
    },
    function (callbackHandler) {
      createSuppliersTable(callbackHandler);
    },
  ],
  function (err) {
    if (err) {
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
