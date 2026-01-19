# Define your environmental variables here; TODO: Update them if needed
BAYERN_CLOUD_API_KEY := $(shell echo $(BAYERN_CLOUD_API_KEY))
AZURE_STORAGE_ACCOUNT_NAME := $(shell echo $(AZURE_STORAGE_ACCOUNT_NAME))
AZURE_STORAGE_ACCOUNT_KEY := $(shell echo $(AZURE_STORAGE_ACCOUNT_KEY))
REPO_PATH := $(shell pwd)
PATH_TO_STREAMLIT_SECRETS := $(shell pwd)/.streamlit/secrets.toml
IMAGE_NAME := bavarian-forest

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Run the Docker container
run:
	docker run \
		-v $(REPO_PATH):/app \
		-v $(PATH_TO_STREAMLIT_SECRETS):/app/.streamlit/secrets.toml \
		-e BAYERN_CLOUD_API_KEY=$(BAYERN_CLOUD_API_KEY) \
		-e AZURE_STORAGE_ACCOUNT_NAME=$(AZURE_STORAGE_ACCOUNT_NAME) \
		-e AZURE_STORAGE_ACCOUNT_KEY=$(AZURE_STORAGE_ACCOUNT_KEY) \
		-p 8501:8501 \
		-t $(IMAGE_NAME)

# Run the Docker container
bash:
	docker run \
		-v $(REPO_PATH):/app \
		-v $(PATH_TO_STREAMLIT_SECRETS):/app/.streamlit/secrets.toml \
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