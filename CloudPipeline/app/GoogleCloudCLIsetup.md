## The steps to download and run google cloud CLI on device/system
**this is because we cannot create JSON keys to connect to the service and API keys don't work with BigQuery

### Step 1: Install google Cloud CLI
Option A - use HomeBrew on mac 
brew install --cask google-cloud-sdk

Option B - manual install
- download from https://cloud.google.com/sdk/docs/install
- then run:  ./google-cloud-sdk/install.sh

### Step 2: restart terminal
either close and reopen or run: source ~/.zshrc

### Step 3: verify installation
run: source ~/.zshrc

### Step 4: initialize gcloud
run: gcloud init
- this will open browser login, let you pick project, set defaults

### Step 5: set up authentication
- because our org blocks JSON keys
- run: gcloud auth application-default login

### Step 6: install the python packages (should be in requirements.txt)
- install if not in requirements: pip install google-cloud-bigquery

### Testing if its successful:
- test test_bigquery.py : python test_bigquery.py
- should have the output if successful: Connected to BigQuery: 1

### MAKE SURE TO CLOSE GOOGLE CLOUD AFTER USE
Log out/remove google account
- stop being authenticated: gcloud auth revoke
- if used ADC: gcloud auth application-default revoke

this is to stop running google cloud API on your system and deactivate it (could be billed otherwise)

### IMPORTANT NOTES
Project ID and account to use is pasted into Teams Chat (its a secret)

### to create with a JSON key !!
Had the ability to create a connection using a service account Key - not really safe since it's a secret
** key is stored in .gitignore so it's not pushed to gitup
What was done to create a key (only needs to be done once for the service account):
- under a created service account with the correct access and permissions > Keys > add key > create new key > JSON > download file
- JSON file that was downloaded should be done so safely.  DO NOT commit to GitHub, add to .gitignore file
- in python code, use the file in a key path variable.  KEY_PATH = "file path"
- OR set an env variable instead of hardcoding path in code
