#!/bin/bash

SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit \
  --class "ru.okabanov.App" \
  --master "local[2]" \
  ${SCRIPT_HOME}/target/challenge-1.0-SNAPSHOT-jar-with-dependencies.jar \
  "$@"
