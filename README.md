## Table of Contents

1. Provision & Connect to EC2
2. Attach & Mount EBS Volume for Redis Data
3. Relocate Docker’s Data Directory to EBS
4. Install Docker & Docker Compose
5. Prepare Redis Configuration & Docker Compose
6. Clone & Containerize FlashSmsRobot
7. Launch & Verify Services
8. Access RedisInsight & Telegram Bot
9. Troubleshooting & Common Fixes
10. EC2 Freezing: Causes & Remedies 10.1 Enable Swap to Prevent OOM 10.2 Regular Maintenance & Monitoring

---

## 1. Provision & Connect to EC2

```bash
# 1.1 Launch an Ubuntu 24.04 LTS instance (t2.micro or t3.small)
#    - Attach a 30 GiB gp2 EBS (unformatted)
#    - Security Group Inbound: 22, 80, 6379, 5540, 8443 → 0.0.0.0/0
#    - Download key: flash-bot-key.pem
ssh -i ~/flash-bot-key.pem ubuntu@<EC2_PUBLIC_IP>
```

---

## 2. Attach & Mount EBS Volume for Redis Data

```bash
# 2.1 Install XFS tools
sudo apt update && sudo apt install -y xfsprogs

# 2.2 Identify the new device (e.g. /dev/xvdb)
lsblk

# 2.3 Format, mount, and persist
sudo mkfs.xfs -f /dev/xvdb
sudo mkdir -p /data/redis
echo '/dev/xvdb /data/redis xfs defaults,nofail 0 2' | sudo tee -a /etc/fstab
sudo mount -a
sudo chown -R ubuntu:ubuntu /data/redis

# 2.4 Verify
lsblk                # /dev/xvdb → /data/redis
df -h /data/redis     # ~30 GiB free
```

---

## 3. Relocate Docker’s Data Directory to EBS

```bash
# 3.1 Stop Docker services
sudo systemctl stop docker

# 3.2 Backup and remove old data (optional)
sudo mv /var/lib/docker /var/lib/docker.old

# 3.3 Create new Docker root on EBS
sudo mkdir -p /data/redis/docker
sudo chown -R root:root /data/redis/docker

# 3.4 Configure Docker to use new root
cat << 'EOF' | sudo tee /etc/docker/daemon.json
{
  "data-root": "/data/redis/docker"
}
EOF

# 3.5 Reload and restart
sudo systemctl daemon-reload
sudo systemctl start docker

# 3.6 Verify
docker info | grep "Docker Root Dir"
# expect: /data/redis/docker

# 3.7 Clean up backup (once confirmed)
sudo rm -rf /var/lib/docker.old
```

---

## 4. Install Docker & Docker Compose

```bash
sudo apt install -y apt-transport-https ca-certificates curl gnupg lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER && newgrp docker

docker version
```

---

## 5. Prepare Redis Configuration & Docker Compose

```bash
cd ~
mkdir -p flashsms-docker && cd flashsms-docker
ls
nano redis.conf
```

**redis.conf**

```ini
# Redis Stack modules
loadmodule /opt/redis-stack/lib/redisearch.so
loadmodule /opt/redis-stack/lib/rejson.so
loadmodule /opt/redis-stack/lib/redisbloom.so
loadmodule /opt/redis-stack/lib/redistimeseries.so

# AOF persistence
appendonly yes
appendfilename "appendonly.aof"
appendfsync always

dir /data
# Disable dangerous commands
rename-command FLUSHALL ""
rename-command FLUSHDB ""
# Network & Security
protected-mode no
bind 0.0.0.0
# Memory
maxmemory 0
maxmemory-policy noeviction
```

```bash
nano docker-compose.yml
```

**docker-compose.yml**

```yaml
version: '3.8'
services:
  redis:
    image: redis/redis-stack-server:latest
    container_name: flashsms-redis
    command: ["redis-server","/usr/local/etc/redis/redis.conf"]
    ports:
      - "0.0.0.0:6379:6379"
    volumes:
      - redis-data:/data
      - ./redis.conf:/usr/local/etc/redis/redis.conf:ro
    restart: unless-stopped
    healthcheck:
      test: ["CMD","redis-cli","ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  redisinsight:
    image: redislabs/redisinsight:latest
    container_name: flashsms-redisinsight
    ports:
      - "0.0.0.0:5540:5540"
    environment:
      - RIPORT=5540
    restart: unless-stopped
    depends_on:
      redis:
        condition: service_healthy

  bot:
    build:
      context: ./FlashSmsRobot
    container_name: flashsms-bot
    env_file:
      - ./FlashSmsRobot/.env
    depends_on:
      redis:
        condition: service_healthy
    restart: unless-stopped

volumes:
  redis-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /data/redis
networks:
  backend:
    driver: bridge
```

---

## 6. Clone & Containerize FlashSmsRobot

```bash
cd ~/flashsms-docker
git clone https://github.com/FlashTheFire/FlashSmsRobot.git
ls FlashSmsRobot
nano FlashSmsRobot/Dockerfile
```

**FlashSmsRobot/Dockerfile**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --upgrade pip && pip install -r requirements.txt
CMD ["python","bot_project/main.py"]
```

> **Tip:** If `requirements.txt` or `.env` is nested, adjust the path or move files accordingly.

---

## 7. Launch & Verify Services

```bash
cd ~/flashsms-docker
docker compose down
docker compose up -d --build
docker compose ps
```

```bash
docker exec -it flashsms-redis redis-cli
> MODULE LIST
# Expect: search, rejson, redisbloom, timeseries
```

```bash
docker logs flashsms-redis
docker logs flashsms-bot | tail -n 20
```

Look for `✅ Combined server started on port 8443` in bot logs.

---

## 8. Access RedisInsight & Telegram Bot

1. Open in browser: `http://<EC2_IP>:5540`
2. Accept EULA & enable Usage Data (optional).
3. **Add Redis Database**:
   - Host: `<EC2_IP>`
   - Port: `6379`
   - Force standalone: ✅
   - Test & Add.

Your Telegram bot will be live on port 8443—ensure your webhook is reachable or switch to polling in `.env`.

---

## 9. Troubleshooting & Common Fixes

- **Docker won’t start**:
  ```bash
  sudo systemctl daemon-reload && sudo systemctl restart docker
  ```
- **Redis protected mode**: already set to `no` in `redis.conf`.
- **Bot Redis error**: ensure `REDIS_HOST=redis` in `.env`, not `127.0.0.1`.
- **Permission errors**: prepend `sudo` (e.g., `sudo dmesg | tail`).
- **Out of disk space**: confirm Docker root and Redis data are on `/data/redis` and `/data/redis/docker`.

---

## 10. EC2 Freezing: Causes & Remedies

| Cause                      | Description                                           | Fix                                                |
| -------------------------- | ----------------------------------------------------- | -------------------------------------------------- |
| Low memory (t2.micro)      | 1 GB RAM may be exhausted under load                  | Upgrade to t3.small or enable swap                 |
| No swap space              | No extra memory buffer → OOM & hangs                  | Create a 2 GB swap (see below)                     |
| CPU credits exhausted      | Burstable instance may throttle CPU                   | Use t3/t3a with Unlimited mode                     |
| Disk full (`/`)            | Root volume full → system processes hang              | Move Docker & Redis to EBS, monitor `df -h`        |
| Corrupt mounts / fs errors | Bad `/etc/fstab` entries or disk corruption           | Use `nofail`, validate fstab, run `fsck` if needed |
| Unwanted auto-restarts     | `restart: unless-stopped` causes containers on reboot | Change to `restart: "no"` in `docker-compose.yml`  |

### 10.1 Enable Swap to Prevent OOM

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
swapon --show
free -h
```

### 10.2 Regular Maintenance & Monitoring

```bash
# Disk & memory
watch -n 5 'free -h && df -h'

# Clean old Docker data weekly (e.g., via cron)
docker system prune -af --filter "until=168h"
```
