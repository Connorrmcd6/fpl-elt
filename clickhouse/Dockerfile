FROM clickhouse/clickhouse-server:latest

# copy SQL / SH init files into the entrypoint init dir
COPY init /docker-entrypoint-initdb.d/

# ensure readable/executable
RUN chmod -R a+rX /docker-entrypoint-initdb.d \
    && chmod +x /docker-entrypoint-initdb.d/01-init_db.sh