#!/bin/bash
# Creates additional databases listed in POSTGRES_MULTIPLE_DATABASES
# Format: "db1,db2,db3"
# The primary DB (POSTGRES_DB) is already created by the postgres image.

set -e

function create_db() {
  local database=$1
  echo "Creating database '$database'"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE $database;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
  echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
  for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
    create_db $db
  done
  echo "Multiple databases created"
fi
