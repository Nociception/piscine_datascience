# Utiliser l'image officielle de PostgreSQL comme base
FROM postgres:latest

# Copier le fichier pg_hba.conf dans le conteneur
COPY pg_hba.conf /docker-entrypoint-initdb.d/pg_hba.conf

# Script pour déplacer pg_hba.conf au bon endroit
RUN echo "#!/bin/bash\n" \
    "cp /docker-entrypoint-initdb.d/pg_hba.conf /var/lib/postgresql/data/pg_hba.conf\n" \
    "chown postgres:postgres /var/lib/postgresql/data/pg_hba.conf" \
    > /docker-entrypoint-initdb.d/init_pg_hba.sh && \
    chmod +x /docker-entrypoint-initdb.d/init_pg_hba.sh
