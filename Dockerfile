FROM postgres
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD docker
ENV POSTGRES_DB pep-base-sql
COPY pep_hub.sql /docker-entrypoint-initdb.d/