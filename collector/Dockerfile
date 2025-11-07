FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    websockets \
    redis

# Copy collector script
COPY collector.py .

# Run collector
CMD ["python", "-u", "collector.py"]
