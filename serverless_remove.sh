#!/usr/bin/env bash

cd imputation-repository
echo Destroying serverless bundle...
serverless remove --verbose;
