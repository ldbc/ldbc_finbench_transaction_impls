#!/bin/bash

SINGLE_FLAG=${1:-false}

if [ "$SINGLE_FLAG" = "true" ]; then
    gsql -g ldbc_fin "install query -single all"
else
    gsql -g ldbc_fin "install query -single trw2"
    gsql -g ldbc_fin "set single_gpr=false install query all"
fi