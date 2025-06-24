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

# Install Python dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Wait for Redis service, then start the bot
ENTRYPOINT ["/usr/local/bin/wait-for-it.sh", "flashsms-redis:6379", "--"]
CMD ["python", "bot_project/main.py"]
