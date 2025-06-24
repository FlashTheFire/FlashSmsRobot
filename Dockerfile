FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install curl and wait-for-it script to coordinate service startup
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && curl -fsSL https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -o /usr/local/bin/wait-for-it.sh \
    && chmod +x /usr/local/bin/wait-for-it.sh \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

# Copy application code and dependencies file
COPY . /app

# Upgrade pip and install Python dependencies without cache to save space
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache

# Wait for Redis service, then start the bot
ENTRYPOINT ["/usr/local/bin/wait-for-it.sh", "flashsms-redis:6379", "--"]
CMD ["python", "bot_project/main.py"]
