FROM postgres:latest

COPY pg_hba.conf /docker-entrypoint-initdb.d/pg_hba.conf
# This directory docker-entrypoint-initdb.d is used to
# customize how the container is started.
# Any file .sql .sh or .sql.gz is run automatically

COPY init_pg_hba.sh /docker-entrypoint-initdb.d/init_pg_hba.sh

RUN chmod +x /docker-entrypoint-initdb.d/init_pg_hba.sh

# The new pg_hba.conf which we copy into the container is designed
# to make authentication mandatory during any connection to the db.
# Read the init_pg_hba.conf notes to learn more.
