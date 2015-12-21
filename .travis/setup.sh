#!/bin/bash
set -e

cd "$(dirname "$0")"

brew remove postgres
brew install postgres

nohup postgres -D /usr/local/var/postgres &
sleep 1 # laaaaame

createuser -s postgres

psql -U postgres < setup.sql

cp pg_hba.conf $(psql -U postgres -c "SHOW hba_file" -At)

DATA_DIR=$(psql -U postgres -c "SHOW data_directory" -At)
PG_PID=$(sudo head -n1 $DATA_DIR/postmaster.pid)
kill -SIGHUP $PG_PID
