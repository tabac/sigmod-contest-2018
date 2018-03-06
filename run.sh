#!/bin/bash

if [ "$#" == "1" ]
then
    BUILDTYPE="debug"
    DATADIR=$(dirname "$1")
    DATAFILE=$(basename "$1")
elif [ "$#" == "2" -a "$1" == "--release" ]
then
    BUILDTYPE="release"
    DATADIR=$(dirname "$2")
    DATAFILE=$(basename "$2")
else
    echo "usage: run.sh <path-to-input-file>"
    exit
fi

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd "${DATADIR}" && "${DIR}/build/${BUILDTYPE}/rocket" < "${DATAFILE}"
