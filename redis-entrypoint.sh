#!/bin/sh
# redis-entrypoint.sh
# Substitutes ${REDIS_PASSWORD} in redis.conf with the actual env var value,
# writes to a temp file, then starts redis-server with that resolved config.
set -e

CONF_SRC="/usr/local/etc/redis/redis.conf"
CONF_DST="/tmp/redis-resolved.conf"

if [ -z "$REDIS_PASSWORD" ]; then
    echo "Error: REDIS_PASSWORD environment variable is not set or empty." >&2
    exit 1
fi

# Use awk for portable substitution — no escaping issues with special chars
awk -v pw="$REDIS_PASSWORD" '{gsub(/\${REDIS_PASSWORD}/, pw)}1' "$CONF_SRC" > "$CONF_DST"

exec redis-server "$CONF_DST"
