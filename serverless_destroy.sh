#!/usr/bin/env bash

cd imputation-deploy-repository
echo Destroying serverless bundle...
serverless remove --verbose;