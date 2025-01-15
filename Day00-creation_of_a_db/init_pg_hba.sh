#!/bin/bash

cp /docker-entrypoint-initdb.d/pg_hba.conf /var/lib/postgresql/data/pg_hba.conf

chown postgres:postgres /var/lib/postgresql/data/pg_hba.conf
chmod 600 /var/lib/postgresql/data/pg_hba.conf

# This .sh file is copied into the /docker-entrypoint-initdb.d directory.
# Read more about this directory in the Dockerfile notes.
# Once run, this file replaces an existing pg_hba.conf file
# in the container, and makes authentication mandatory during
# any db connection.
