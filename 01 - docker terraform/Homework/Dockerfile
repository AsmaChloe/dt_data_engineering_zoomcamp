FROM python:3.9

# Install packages
RUN apt-get update && apt-get install -y wget && \
    pip install pandas sqlalchemy psycopg2

# Define the working directory as /app
WORKDIR /app

# Copy the Python script into the container
COPY ingest_data.py ingest_data.py

# Run the Python script with arguments when the container is launched
ENTRYPOINT python ingest_data.py --user=$POSTGRES_USER --password=$POSTGRES_PASSWORD --host=$POSTGRES_HOST --port=5432 --db=$POSTGRES_DB
