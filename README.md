Sumo Logic Azure Functions [![Build Status](https://travis-ci.org/SumoLogic/sumologic-azure-function.svg?branch=master)](https://travis-ci.org/SumoLogic/sumologic-azure-function)
==============================

# Introduction
This repository contains a collection of Azure functions to collect data and send to Sumo Logic cloud service, and a library called sumo-function-utils for these functions.

Following integrations are present. For more info look at their respective ReadMe files.

* Sumo Logic Azure Event Hub Integration
* Sumo Logic Azure Blob Storage Integration

## For Developers
Each integration is structured in three folders
src/     - contains actual source files
target/  - directory used by azure's github integration to fetch source code
tests/   - contains integration tests
