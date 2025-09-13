# /var/www/LA-Rest-Data/setup_commands.sh
#!/bin/bash
echo "Setting up LA Restaurant Data project..."

# Create directory structure
mkdir -p {config,src,data/{raw,processed,backup},logs,docs,powerbi}

# Create requirements.txt
cat > requirements.txt << 'EOL'
requests==2.31.0
pandas==2.0.3
python-dotenv==1.0.0
openpyxl==3.1.2
EOL