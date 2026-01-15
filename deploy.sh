#!/bin/bash
set -e

# -------------------------
# Variables (edit as needed)
# -------------------------
PROJECT_DIR="/home/ec2-user/mediaflow"
VENV_DIR="$PROJECT_DIR/venv"
GUNICORN_SERVICE="/etc/systemd/system/fastapi.service"
NGINX_CONF="/etc/nginx/conf.d/fastapi.conf"
FFMPEG_DIR="/usr/local/bin"
APP_MODULE="main:app"    # Adjust if your entrypoint is different
WORKERS=2

# -------------------------
# 1. Update system
# -------------------------
sudo dnf update -y

# -------------------------
# 2. Install dependencies
# -------------------------
sudo dnf install -y python3 python3-venv python3-pip git nginx

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

# Verify FFmpeg
ffmpeg -version

# -------------------------
# 4. Setup project & virtual environment
# -------------------------
cd $PROJECT_DIR

python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install gunicorn

# -------------------------
# 5. Setup systemd service
# -------------------------
sudo tee $GUNICORN_SERVICE > /dev/null <<EOF
[Unit]
Description=FastAPI Application
After=network.target

[Service]
User=ec2-user
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$VENV_DIR/bin"
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
sudo systemctl start fastapi

# -------------------------
# 6. Setup Nginx
# -------------------------
sudo tee $NGINX_CONF > /dev/null <<EOF
server {
    listen 80 default_server;
    server_name _;

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

# Remove default conf to avoid conflicts
sudo rm -f /etc/nginx/conf.d/default.conf

# Test and restart Nginx
sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

# -------------------------
# 7. Final message
# -------------------------
echo "==========================================="
echo "Deployment complete!"
echo "FastAPI should now be accessible at:"
echo "http://<EC2_PUBLIC_IP>/docs"
echo "Gunicorn is bound to 127.0.0.1:8000 and proxied via Nginx"
echo "FFmpeg installed at $FFMPEG_DIR/ffmpeg"
echo "==========================================="
