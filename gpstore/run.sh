#!/usr/bin/env bash
# Usage: ./run.sh [config-name]
#   config-name: properties file name (without .properties) under src/main/resources/
#                default: opt1
#   e.g. ./run.sh opt2
CONFIG="${1:-opt1}"
java -cp target/gpstore-0.2.0-alpha.jar org.ldbcouncil.finbench.driver.driver.Driver -P "src/main/resources/${CONFIG}.properties"
