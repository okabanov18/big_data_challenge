#!/bin/bash

SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
  --class "ru.okabanov.challenge.IotLogsStreamingProcessing" \
  --master "yarn" \
  --properties-file spark_application.conf \
  ${SCRIPT_HOME}/target/challenge-1.0-SNAPSHOT-jar-with-dependencies.jar \
  "$@"
