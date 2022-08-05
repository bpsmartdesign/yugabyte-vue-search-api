#!/bin/bash

cd /usr/local/yugabyte-search-product

# Init the database.
node models/yugabyte/db_init.js

# Start the rest service
yarn start
