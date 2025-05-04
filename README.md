# Run the Bavarian Forest code locally for testing

Follow the steps below to run the Bavarian Forest code locally. This is used for internal testing and debugging.

**Step 1:** Clone the run model_locally branch of the Bavarian Forest repository
```bash

git clone -b run_model_locally --single-branch https://github.com/DSSGxMunich/bavarian-forest-visitor-monitoring-dssgx-24.git
```
**Step 2:** Install the required packages
```bash
pip install -r requirements.txt
```
*It is recommended to use a virtual environment for this step. You can create a virtual environment using the following command:*
```bash 
conda create -n <env_name> python=3.10
```
Then activate the environment using:
```bash
conda activate <env_name>
```
**Step 3:** Run the code
```bash
streamlit run run_app.py --train
```
If you want to run the code without training, use the following command:
```bash
streamlit run_app.py 
```

*Data Access section of the dashboard is not working yet.*