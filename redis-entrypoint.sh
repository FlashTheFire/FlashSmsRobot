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

# Secure the destination file permissions before writing so the password isn't exposed
touch "$CONF_DST"
chmod 600 "$CONF_DST"

# Use pure awk literal replacement (index/substr) so special characters in the password (like & or \) are not misinterpreted by gsub
awk '
BEGIN { target = "${REDIS_PASSWORD}"; pw = ENVIRON["REDIS_PASSWORD"] }
{
    if ((idx = index($0, target)) != 0) {
        $0 = substr($0, 1, idx - 1) pw substr($0, idx + length(target))
    }
    print
}' "$CONF_SRC" > "$CONF_DST"

exec redis-server "$CONF_DST"
