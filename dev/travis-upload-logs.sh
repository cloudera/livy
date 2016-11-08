#!/bin/bash

set -e

if [ -z "$TRAVIS_JOB_NUMBER" ]; then
  echo "TRAVIS_JOB_NUMBER isn't defined."
  exit 1
fi

if [ -z "$LOG_AZURE_STORAGE_CONNECTION_STRING" ]; then
  echo "LOG_AZURE_STORAGE_CONNECTION_STRING isn't defined."
  exit 1
fi

export AZURE_STORAGE_CONNECTION_STRING="$LOG_AZURE_STORAGE_CONNECTION_STRING"

LOG_ZIP_NAME="$TRAVIS_JOB_NUMBER.zip"
LOG_ZIP_PATH="assembly/target/$LOG_ZIP_NAME"

find * -name "*.log" -o -name "stderr" -o -name "stdout" | zip -@ $LOG_ZIP_PATH

azure telemetry --disable
azure storage blob upload -q $LOG_ZIP_PATH buildlogs $LOG_ZIP_NAME && echo "===== Build log uploaded to https://livy.blob.core.windows.net/buildlogs/$LOG_ZIP_NAME ====="
