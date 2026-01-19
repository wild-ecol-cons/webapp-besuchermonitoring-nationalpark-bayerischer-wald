![Bavarian Forest Logo](src/streamlit_app/assets/logo-bavarian-forest-national-park.png)
# Harmonizing Tourism and Nature Protection in the Bavarian Forest National Park 🌲 

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_red.svg)](https://nationalpark-bayerischer-wald.streamlit.app/)![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square) [![License: MPL 2.0](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/license/mit) ![Python](https://img.shields.io/badge/python-3.10-blue.svg)

This repository includes the code and documentation for the project "*Harmonizing tourism and nature protection in the Bavarian Forest National Park*" by the fellowship program [Data Science for Social Good Munich, 2024](https://sites.google.com/view/dssgx-munich-2023/startseite).

## Project Overview 🌍 

### Background 📜

The Bavarian Forest National Park is a protected area in the Bavarian Forest in Bavaria, Germany. Since its foundation, the park has been a place for nature conservation and research. The park is also a popular tourist destination, attracting visitors over 1.4 million visitors per year from all over the world. The park is home to a wide variety of flora and fauna, including many rare and endangered species.

### Problem Statement 🎯

The park faces the challenge of balancing the needs of nature conservation with the demands of tourism. The park has installed a network of sensors (26 visitor counters and 12 parking sensors) to understand the flow of visitors which will optimize the visitor experience and protect the park's natural resources. This data is collected heterogeneously and needs to be unified and harmonized to provide insights for decision-making.

### Project Goal and Contributions 🚀

The goal of this project is to harmonize the data collected from the sensors in the Bavarian Forest National Park to provide insights for decision-making. 
We contribute to the project in the following ways:
1. Develop a data pipeline to harmonize the data collected from the all the different sensors and external sources.
2. Implement a predictive model to forecast the visitor traffic in the park for the coming week.
3. Develop a dashboard to visualize the data and insights for the park management, along with visualizing the forecasted visitor traffic from the predictive model.
4. Create technical documentation to provide insights on the data pipeline, predictive model and suggestions for future improvements in the project.

![Overall Solution](docs/asset/overall-dashboard.gif)
_A glimpse of the final dashboard ✨_

## How to use the code

### Run Dashboard and Pipeline via a Docker Container

1. Clone the repository:
   ```bash
   git clone https://github.com/DSSGxMunich/bavarian-forest-visitor-monitoring-dssgx-24.git
    ```
2. Download Docker Desktop from [here](https://www.docker.com/products/docker-desktop/) and install it.

3. **Run the container via make:**

    a. **Make sure to have `make` installed.** If not, install it (e.g., with Homebrew for macOS - `brew install make`).

    b. **Authenticate with Azure:** As the project is loading and writing data to a configured Azure Blob Storage Container, add Azure credentials (`AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`) to the [Makefile](Makefile), or specify to load them as environmental variables (as currently set up and the preferred option due to security reasons).

    c. **Run the Dashboard:** Run the following command to build and run the Streamlit dashboard:
    ```bash
    make streamlit
    ```
    [!NOTE]  If you want to run the bash shell in the docker container, run the following command:

    ```bash
    make container
    ```

    If you then want to debug the Streamlit Dashboard while in container mode, run the following command inside the container:

    ```bash
    streamlit run Dashboard.py
    ```
    
4. **Run the container without `make`:**

    a. **Authenticate with Azure:** As the project is loading and writing data to a configured Azure Blob Storage Container, add Azure credentials (`AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`) to the [Makefile](Makefile), or specify to load them as environmental variables (as currently set up and the preferred option due to security reasons).

    b. **Run the `docker build` command** to first build the Docker image needed to run the code. For further details, check the `Makefile`.

    c. **Run the `docker run` command** to run a container based on the previously built image. For further details on how to specify the `run` command, check the [Makefile](Makefile).

### Run Notebooks and General Code in a Local, Virtual Environment

Choose a virtual environment tool of your choice and install the dependencies of the respective requirements file, e.g. [requirements.txt](requirements.txt) (for the dashboard), [notebooks-requirements.txt](notebooks-requirements.txt) (for the notebooks), or [docs-requirements.txt](docs-requirements.txt) (for the documentation) from the root of the repository. In the following, you see the steps to create a virtual environment with a particularly, specified Python version with `pyenv` and the plugin `pyenv-virtualenv`.

1.  Install `pyenv-virtualenv`. Follow a tutorial to install `pyenv` and the plugin `pyenv-virtualenv`, e.g. follow [this tutorial](https://medium.com/@adocquin/mastering-python-virtual-environments-with-pyenv-and-pyenv-virtualenv-c4e017c0b173).

2. Create a virtual environment with a specified Python version.
    ```bash
    pyenv virtualenv {selected-python-version} {name-of-virtual-environment}
    ```
3. Activate the virtual environment.
    ```bash
    pyenv activate {name-of-virtual-environment}
    ```
4. Install the dependencies of the [requirements.txt](requirements.txt) in the root of the repository.
    ```bash
    pip install -r requirements.txt
    ```
#### Run Jupyter Notebooks

1. **Add the virtual environment as Jupyter kernel**

    In order to be able to run Jupyter notebooks in the created virtual environment, you need to specify a new kernel to be used by Jupyter making use of your virtual environment. Run the following command in the CLI by specifying a name for the kernel:

    ```
    python -m ipykernel install --user --name={name-for-kernel}
    ```

2. **Open Jupyter notebook**

    Either run the following command in the CLI to trigger the pop-up of the Jupyter interface in your browser:

    ```
    jupyter notebook
    ```

    OR: In case you are keen on using Jupyter Notebooks in the IDE VS Code, open the Jupyter Notebook in VS Code. (Tip: this way you can use all other VS Code features in notebooks, for example nice code highlighting, AI Coding features, etc.)

3. **Select kernel**

    In both the Jupyter UI and the notebook in VS Code, you need to select the specified kernel from before running the cells.

4. **Run the notebooks**

    Now go ahead and run the notebooks! :)

## Structure of the Repository 📁

The repository is structured as follows:

```
bavarian-forest-visitor-monitoring-dssgx-24/

|
├── .streamlit/             # Contains the configurations for the Streamlit Dashboard
│
├── docs/                   # Contains the technical documentation
│
├── notebooks/              # Contains the code notebooks developed during exploration and experimentation of the project
│
├── src/                    # Contains the source code for the prediction pipeline and the Streamlit Dasbhoard
│
├── pages/                  # Contains the code for the additional pages for the multi-page Streamlit dashboard
│
├── .gitignore              # Defines the files that are not being tracked with Git
│
└── Dashboard.py            # Contains the code for the Homepage of the Streamlit dashboard
│
└── Dockerfile              # Contains the instructions in order to build and run the Docker container
│
├── Makefile                # Contains the Docker commands to run the code
│
├── README.md               # Contains the information about the project
│
└── docs-requirements.txt   # Contains the dependencies for building the technical documentationlocally
│
└── mkdocs.yml              # Contains the configurations for the technical documentation with Mkdocs
│
└── notebooks-requirements.txt  # Contains the dependencies for running the code notebooks
│
├── requirements.txt        # Contains the dependencies for running the Streamlit dashboard application

```

## Current Deployment / Infrastructure Setup

Currently, the web app is being hosted and managed via Azure. The following infrastructure is used:

- App Service plan to host the web app on Azure
- Container registry to have containerized apps enlisted
- App Service to provide the infrastructure to host the web app on Azure
- Storage account to host the data of the project via Azure Blob Storage

## How to Deploy a New Version of the Web App 🚀

As mentioned, the web app is hosted via Azure App Service in form of a container. To deploy a new version of the web app, you need to follow these steps:

1. Follow the steps of [local code setup](#how-to-use-the-code).
2. Do changes to the code and test them locally.
3. From root, locally build the Docker container for the expected Linux platform with the Azure-specific Dockerfile with the following command:

    ```
    docker buildx build -f Dockerfile-azure --platform linux/amd64 -t besuchermonitoring.azurecr.io/webapp:{version-tag} .
    ```
4. Login via the CLI into Azure and push the image to the container registry:

    ```
    az acr login --name besuchermonitoring
    docker push besuchermonitoring.azurecr.io/webapp:{version-tag}
    ```
5. In the Azure portal, go to the App Service service and to "Deployment Center". Here, select the new version of the successfully pushed Docker image and deploy it.

6. After some time, the new version of the app should be available.

## Technical Documentation 📚

The technical documentation website is available [here](https://dssgxmunich.github.io/bavarian-forest-visitor-monitoring-dssgx-24/). 

### Update the Docs & Build the Docs Locally

In order to update information in the docs and test them, follow these steps:

1. Make sure you have the needed libraries to setup `Mkdocs`. For that, you need to install the [docs-requirements.txt](docs-requirements.txt) (Tip: Create a new virtual environment for this.)

2. You want to locally test the documentation and see changes live reflected? Run the following command from the root of the repository:

    ```
    mkdocs serve
    ```

3. You are satisfied with your results and updates to the technical documentation, you have inspected it locally, and now want to make the information available to all users? Run the following command from the root of the repository:

    ```
    mkdocs gh-deploy
    ```

## How to Contribute to the Project 🤝

You can find [here](https://github.com/DSSGxMunich/bavarian-forest-visitor-monitoring-dssgx-24/issues) an overview of open issues. Feel free to have a look and contribute to support this open-source project! 🤗 Follow the following steps to contribute to the project:

1. Fork the repository to your GitHub account.
2. Create a new branch with a descriptive name for the feature you want to contribute to.
3. Make changes to the code or documentation.
4. Commit the changes to your branch.
5. Push the changes to your forked repository.
6. Create a pull request to this main repository.

NOTE: Be sure to merge the latest from the `upstream` before making a pull request!

### Requesting new features or reporting bugs 🐞

If you have any suggestions for new features or find any bugs, please [create an issue](https://github.com/DSSGxMunich/bavarian-forest-visitor-monitoring-dssgx-24/issues/new) in the repository. We are open to feedback and contributions! 🙏

## License 📝

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


