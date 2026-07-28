# Define your environmental variables here; TODO: Update them if needed
REPO_PATH := $(shell pwd)
SECRETS_FILE := $(shell pwd)/.streamlit/secrets.toml
IMAGE_NAME := bavarian-forest

# Construct necessary environment variables from the secrets file
BAYERN_CLOUD_API_KEY := $(shell grep '^BAYERN_CLOUD_API_KEY' $(SECRETS_FILE) | sed 's/.*= *"\(.*\)"/\1/')
AZURE_STORAGE_ACCOUNT_NAME := $(shell grep '^AZURE_STORAGE_ACCOUNT_NAME' $(SECRETS_FILE) | sed 's/.*= *"\(.*\)"/\1/')
AZURE_STORAGE_ACCOUNT_KEY := $(shell grep '^AZURE_STORAGE_ACCOUNT_KEY' $(SECRETS_FILE) | sed 's/.*= *"\(.*\)"/\1/')


# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Run the Docker container
run:
	docker run \
		-v $(REPO_PATH):/app \
		-v $(SECRETS_FILE):/app/.streamlit/secrets.toml \
		-e BAYERN_CLOUD_API_KEY=$(BAYERN_CLOUD_API_KEY) \
		-e AZURE_STORAGE_ACCOUNT_NAME=$(AZURE_STORAGE_ACCOUNT_NAME) \
		-e AZURE_STORAGE_ACCOUNT_KEY=$(AZURE_STORAGE_ACCOUNT_KEY) \
		-p 8501:8501 \
		-t $(IMAGE_NAME)

# Run the Docker container
bash:
	docker run \
		-v $(REPO_PATH):/app \
		-v $(SECRETS_FILE):/app/.streamlit/secrets.toml \
		-e BAYERN_CLOUD_API_KEY=$(BAYERN_CLOUD_API_KEY) \
		-e AZURE_STORAGE_ACCOUNT_NAME=$(AZURE_STORAGE_ACCOUNT_NAME) \
		-e AZURE_STORAGE_ACCOUNT_KEY=$(AZURE_STORAGE_ACCOUNT_KEY) \
		-p 8501:8501 \
		-it --entrypoint /bin/bash $(IMAGE_NAME)


# Combined build and run
streamlit: build run

# Combined build and bash
container: build bash

# Combined build and sso-bash
sso-container: build sso-bash