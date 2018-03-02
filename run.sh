#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

DATADIR=$(dirname "$1")
DATAFILE=$(basename "$1")

cd ${DATADIR} && ${DIR}/build/release/Joiner < ${DATAFILE}
