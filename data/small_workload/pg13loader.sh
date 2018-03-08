#!/bin/bash

createuser \
    --superuser \
    "$(whoami)"

createdb \
    --owner "$(whoami)" \
    sigmod

psql --dbname sigmod < allRelations.sql
