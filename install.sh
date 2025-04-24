#!/bin/bash

# install.sh - Installer for templogger

set -e

SCRIPT_NAME="templogger.py"
SCRIPT2_NAME="inception-server.py"
SERVICE_NAME="templogger.service"
SERVICE2_NAME="inception-server.service"
TIMER_NAME="templogger.timer"
CONFIG_EXAMPLE="example.config.json"
CONFIG_DIR="/etc/templogger"
CONFIG_FILE="$CONFIG_DIR/config.json"
VENV_DIR="/opt/templogger-venv"
REQUIREMENTS="requirements.txt"
WORKDIR="/opt/templogger"

# Check if the script is run as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root or use sudo."
    exit 1
fi

# Check if the script is run from the correct directory
if [ ! -f "$SCRIPT_NAME" ]; then
    echo "Error: $SCRIPT_NAME not found in the current directory."
    exit 1
fi
if [ ! -f "$REQUIREMENTS" ]; then
    echo "Error: $REQUIREMENTS not found in the current directory."
    exit 1
fi
if [ ! -f "$CONFIG_EXAMPLE" ]; then
    echo "Error: $CONFIG_EXAMPLE not found in the current directory."
    exit 1
fi
if [ ! -f "$SERVICE_NAME" ]; then
    echo "Error: $SERVICE_NAME not found in the current directory."
    exit 1
fi
if [ ! -f "$TIMER_NAME" ]; then
    echo "Error: $TIMER_NAME not found in the current directory."
    exit 1
fi

# Check that python3 and pip are installed
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed. Please install it and try again."
    exit 1
fi
if ! command -v pip3 &> /dev/null; then
    echo "Error: pip3 is not installed. Please install it and try again."
    exit 1
fi

echo "==> 1.1 Creating config directory at $CONFIG_DIR"
sudo mkdir -p "$CONFIG_DIR"

echo "==> 1.2 Creating working directory at $WORKDIR"
sudo mkdir -p "$WORKDIR"
sudo chown "$USER":"$USER" "$WORKDIR"
sudo chmod 755 "$WORKDIR"

echo "==> 2.1 Copying $SCRIPT_NAME to /usr/local/bin/"
sudo cp "$SCRIPT_NAME" /usr/local/bin/
sudo chmod +x /usr/local/bin/"$SCRIPT_NAME"

echo "==> 2.2 Copying $SCRIPT2_NAME to /usr/local/bin/"
sudo cp "$SCRIPT2_NAME" /usr/local/bin/
sudo chmod +x /usr/local/bin/"$SCRIPT2_NAME"

echo "==> 3. Copying $CONFIG_EXAMPLE to $CONFIG_FILE"
sudo cp "$CONFIG_EXAMPLE" "$CONFIG_FILE"
sudo chown root:root "$CONFIG_FILE"
sudo chmod 600 "$CONFIG_FILE"

echo "==> 4. Creating Python virtual environment at $VENV_DIR"
sudo python3 -m venv "$VENV_DIR"

echo "==> 5. Installing requirements into virtual environment"
sudo "$VENV_DIR/bin/pip" install --upgrade pip
sudo "$VENV_DIR/bin/pip" install -r "$REQUIREMENTS"

echo "==> 6. Installing systemd services and timer"
sudo cp "$SERVICE_NAME" /etc/systemd/system/
sudo cp "$TIMER_NAME" /etc/systemd/system/
sudo cp "$SERVICE2_NAME" /etc/systemd/system/

echo "==> 7. Reloading systemd daemon and enabling timer"
sudo systemctl daemon-reload
sudo systemctl enable --now "$TIMER_NAME"

echo
echo "Installation complete."
echo
echo "IMPORTANT:"
echo " - Add your Google Cloud service account key file and update the 'service_account_key_path' in $CONFIG_FILE."
echo " - Edit the configuration file ($CONFIG_FILE) to suit your environment and sensors."
echo
echo "To check status:"
echo "  sudo systemctl status $SERVICE_NAME"
echo "  sudo systemctl status $TIMER_NAME"
