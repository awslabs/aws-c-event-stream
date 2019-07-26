#!/bin/bash

# Until CodeBuild supports macOS, this script is just used by Travis.

set -e

CMAKE_ARGS="$@ -DCMAKE_INSTALL_PREFIX=../../install -DCMAKE_PREFIX_PATH=../../install -DENABLE_SANITIZERS=ON"

function install_library {
    git clone https://github.com/awslabs/$1.git
    cd $1
    mkdir build
    cd build

    cmake $CMAKE_ARGS ../
    make install

    cd ../..
}

cd ../

mkdir install

install_library aws-c-common
install_library aws-checksums

cd aws-c-event-stream
mkdir build
cd build
cmake $CMAKE_ARGS ../

make

LSAN_OPTIONS=verbosity=1:log_threads=1 ctest --output-on-failure
