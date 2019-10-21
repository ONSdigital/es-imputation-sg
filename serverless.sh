#!/usr/bin/env bash

cd imputation-deploy-repository
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;