#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd "$DIR"

if [ "$#" == "1" -a "$1" == "--release" ]
then
    BUILDDIR="build/release"
    BUILDTYPE="Release"
else
    BUILDDIR="build/debug"
    BUILDTYPE="Debug"
fi

mkdir -p "${BUILDDIR}"
cd "${BUILDDIR}"
cmake -DCMAKE_BUILD_TYPE="${BUILDTYPE}" -DFORCE_TESTS=OFF ../..
make -j8
