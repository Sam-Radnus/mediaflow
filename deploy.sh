#!/bin/bash
set -e

# -------------------------
# Validate input
# -------------------------
if [ -z "$1" ]; then
  echo "Usage: $0 <EC2_PUBLIC_IP>"
  exit 1
fi

EC2_PUBLIC_IP="$1"

# -------------------------
# Variables
# -------------------------
GIT_REPO_URL="https://github.com/Sam-Radnus/mediaflow.git"
PROJECT_DIR="/home/ec2-user/mediaflow"
VENV_DIR="$PROJECT_DIR/venv"
GUNICORN_SERVICE="/etc/systemd/system/fastapi.service"
NGINX_CONF="/etc/nginx/conf.d/fastapi.conf"
FFMPEG_DIR="/usr/local/bin"
DOCKER_CONFIG_DIR="/usr/local/lib/docker/cli-plugins"
APP_MODULE="main:app"
WORKERS=2

# -------------------------
# 1. Update system
# -------------------------
sudo dnf update -y

# -------------------------
# 2. Install dependencies
# -------------------------
sudo dnf install -y git nginx docker

# -------------------------
# 2.5 Configure Docker & Install Compose Binary
# -------------------------
echo "Configuring Docker and installing Compose..."

# Start and enable Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Add ec2-user to docker group so the app can use it
sudo usermod -aG docker ec2-user

# Install Docker Compose Plugin
sudo mkdir -p $DOCKER_CONFIG_DIR
COMPOSE_URL="https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)"
sudo curl -SL $COMPOSE_URL -o $DOCKER_CONFIG_DIR/docker-compose
sudo chmod +x $DOCKER_CONFIG_DIR/docker-compose

# Verify Docker Compose works
docker compose version

# -------------------------
# 3. Install FFmpeg (static binary)
# -------------------------
FFMPEG_URL="https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
TMP_DIR="/tmp/ffmpeg-static"

mkdir -p $TMP_DIR
curl -L $FFMPEG_URL -o $TMP_DIR/ffmpeg.tar.xz
tar -xf $TMP_DIR/ffmpeg.tar.xz -C $TMP_DIR --strip-components=1
sudo mv $TMP_DIR/ffmpeg $FFMPEG_DIR/
sudo mv $TMP_DIR/ffprobe $FFMPEG_DIR/
rm -rf $TMP_DIR

ffmpeg -version

# -------------------------
# 4. Clone or update project
# -------------------------
if [ ! -d "$PROJECT_DIR/.git" ]; then
  sudo -u ec2-user git clone $GIT_REPO_URL $PROJECT_DIR
else
  cd $PROJECT_DIR
  sudo -u ec2-user git pull
fi

cd $PROJECT_DIR

# -------------------------
# 5. Setup virtual environment
# -------------------------
# Note: creating venv as ec2-user to avoid permission issues later
sudo -u ec2-user python3 -m venv $VENV_DIR
sudo -u ec2-user $VENV_DIR/bin/pip install --upgrade pip
sudo -u ec2-user $VENV_DIR/bin/pip install -r requirements.txt
sudo -u ec2-user $VENV_DIR/bin/pip install gunicorn

# -------------------------
# 6. Setup systemd service
# -------------------------
sudo tee $GUNICORN_SERVICE > /dev/null <<EOF
[Unit]
Description=FastAPI Application
After=network.target docker.service

[Service]
User=ec2-user
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$VENV_DIR/bin:/usr/bin:/usr/local/bin"
ExecStart=$VENV_DIR/bin/gunicorn $APP_MODULE \
    -k uvicorn.workers.UvicornWorker \
    --bind 127.0.0.1:8000 \
    --workers $WORKERS
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable fastapi
sudo systemctl restart fastapi

# -------------------------
# 7. Setup Nginx
# -------------------------
sudo tee $NGINX_CONF > /dev/null <<EOF
server {
    listen 80;
    server_name $EC2_PUBLIC_IP;

    client_max_body_size 5G;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;

        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;

        proxy_request_buffering off;
        proxy_buffering off;
    }
}
EOF

sudo rm -f /etc/nginx/conf.d/default.conf

sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

# -------------------------
# 8. Final message
# -------------------------
echo "==========================================="
echo "Deployment complete"
echo "Application URL: http://$EC2_PUBLIC_IP/docs"
echo "Docker Compose version: $(docker compose version)"
echo "FFmpeg path: $FFMPEG_DIR/ffmpeg"
echo "==========================================="∂