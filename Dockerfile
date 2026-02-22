# Install Python base image
FROM python:3.10

# Create folder for source code and make it working directory
RUN mkdir -p /app
WORKDIR /app

# Install the certificate store
USER root
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Point the underlying SSL libraries to the correct path
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

# Install the python requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Run pipeline and dashboard entry script
CMD ["streamlit", "run", "Dashboard.py"]