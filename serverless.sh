#!/usr/bin/env bash
cd deploy-repository

echo Packaging serverless bundle...

serverless package --package pkg
serverless deploy --verbose;