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
export TEMPLATE_NAME="azuredeploy_metrics.json"
python test_eventhub_metrics.py
# For deleting leftover resources in case of failures
# python ~/git/sumologic-azure-function/deletetestresourcegroups.py