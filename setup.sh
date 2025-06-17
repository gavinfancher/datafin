#!/bin/bash

# Prompt for AWS Access Key
read -p "AWS Access Key: " aws_access_key

# Prompt for AWS Secret Access Key
read -p "AWS Secret Access Key: " aws_secret_key

# Create .env file
cat > .env << EOF
# AWS credentials
PERSONAL_AWS_ACCESS_KEY="$aws_access_key"
PERSONAL_AWS_SECRET_ACCESS_KEY="$aws_secret_key"
EOF

echo "

.env file created

"

# Check if we're in datafin directory
if [[ ! -d "datafin-dagster" ]]; then
    echo "Error: datafin-dagster directory not found. Make sure you're in the datafin directory."
    exit 1
fi

# Create tmux session and run setup commands
tmux new-session -d -s live -c "$PWD" \; \
    send-keys 'python3.12 -m venv datafin-venv' Enter \; \
    send-keys 'source datafin-venv/bin/activate' Enter \; \
    send-keys 'pip install -r requirements.txt' Enter \; \
    attach-session -t live