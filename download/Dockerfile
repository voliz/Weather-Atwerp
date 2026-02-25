FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy download script
COPY download_data.py .

# Run the download script
CMD ["python", "download_data.py"]
