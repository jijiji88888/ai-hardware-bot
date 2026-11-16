FROM python:3.11-slim

# Keep Python from writing .pyc files / buffering stdout (so `docker logs` is live)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install sqlite3 CLI for WAL inspection / backups and create non-root user
RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin botuser \
    && mkdir -p /app/data \
    && chown -R botuser:botuser /app

# Install deps first so code edits don't bust the layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy both app.py and the pure-ingestion daemon entrypoint aggregator_bridge.py
COPY --chown=botuser:botuser app.py aggregator_bridge.py ./

USER botuser

# Matches HEALTHCHECK_FILE in .env — flags a stuck container without needing to tail logs
HEALTHCHECK --interval=5m --timeout=10s --start-period=2m --retries=3 \
    CMD find /app/data/heartbeat.txt -mmin -15 | grep -q . || exit 1

ENTRYPOINT ["python", "-u", "aggregator_bridge.py"]
