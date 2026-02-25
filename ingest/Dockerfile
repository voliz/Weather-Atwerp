FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script
COPY load_raw_data.py .

# Run the ingestion script
CMD ["python", "load_raw_data.py"]
