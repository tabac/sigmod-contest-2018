#!/bin/bash

if [ "$#" != "1" ]
then
    echo "usage: run.sh <path-to-input-file>"
    exit
fi
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

DATADIR=$(dirname "$1")
DATAFILE=$(basename "$1")

cd "${DATADIR}" && "${DIR}/build/release/rocket" < "${DATAFILE}"
