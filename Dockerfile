FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies for hot reload
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && pip install --upgrade pip watchdog \
    # Grab wait-for-it for service orchestration
    && curl -fsSL https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -o /usr/local/bin/wait-for-it.sh \
    && chmod +x /usr/local/bin/wait-for-it.sh \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements to leverage cache
COPY requirements.txt /app/requirements.txt

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the code (mounted in dev)
COPY . /app

# Wait for Redis, then start bot with auto-reload
ENTRYPOINT ["/usr/local/bin/wait-for-it.sh", "flashsms-redis:6379", "--"]
CMD ["watchmedo", "auto-restart", "--directory=/app", "--pattern=*.py", "--recursive", "--", "python", "bot_project/main.py"]
