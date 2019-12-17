#!/usr/bin/env bash

cd imputation-deploy-repository
echo Installing dependancies
serverless plugin install --name serverless-latest-layer-version
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;