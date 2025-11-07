FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for cron and gcloud
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    curl \
    gnupg \
    lsb-release

# Install the Google Cloud CLI
RUN curl -sS https://dl.google.com/dl/cloudsdk/channels/rapid/google-cloud-sdk.tar.gz | tar -xz -C /usr/local/ && \
    /usr/local/google-cloud-sdk/install.sh --quiet && \
    ln -s /usr/local/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud

# Install Python dependencies
RUN pip install --no-cache-dir \
    redis \
    pandas \
    pyarrow

# Copy all necessary scripts and files
COPY persister.py .
COPY cleanup_data.sh .
COPY cleanup-cron /etc/cron.d/cleanup-cron
COPY entrypoint.sh .

# Give execution rights to the scripts and set up cron
RUN chmod 0644 /etc/cron.d/cleanup-cron && \
    crontab /etc/cron.d/cleanup-cron && \
    chmod +x cleanup_data.sh && \
    chmod +x entrypoint.sh && \
    # Create a log file for cron jobs
    touch /var/log/cron.log

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Run the persister script by default (this command is passed to the entrypoint)
CMD ["python", "-u", "persister.py"]