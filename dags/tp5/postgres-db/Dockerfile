# Use the official PostgreSQL image from Docker Hub
FROM postgres

# Set the environment variable for the PostgreSQL password
ENV POSTGRES_PASSWORD postgres_password

# Copy a custom initialization script to the container
COPY init.sql /docker-entrypoint-initdb.d/

# Expose the default PostgreSQL port
EXPOSE 5432
