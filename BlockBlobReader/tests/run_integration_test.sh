#!/bin/bash

export AZURE_SUBSCRIPTION_ID=""
# application id
export AZURE_CLIENT_ID=""
export AZURE_CLIENT_SECRET=""
export AZURE_TENANT_ID=""
export AZURE_DEFAULT_REGION="eastus"
export SUMO_ACCESS_ID=""
export SUMO_ACCESS_KEY=""
export SUMO_DEPLOYMENT="us1"
export TEMPLATE_NAME="blobreaderdeploy.json"
# export FIXTURE_FILE="blob_fixtures.json"
export FIXTURE_FILE="blob_fixtures_vnetflowlogs.json"
# export FIXTURE_FILE="blob_fixtures_subnetflowlogs.json"
# export FIXTURE_FILE="blob_fixtures_networkinterfaceflowlogs.json"
export MAX_FOLDER_DEPTH=1
# export TEMPLATE_NAME="blobreaderdeploywithPremiumPlan.json"
# export TEMPLATE_NAME="blobreaderzipdeploy.json"
python test_blobreader.py
# python ~/git/sumologic-azure-function/deletetestresourcegroups.py
