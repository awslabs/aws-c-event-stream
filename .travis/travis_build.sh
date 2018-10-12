#!/bin/bash
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#
set -ex

PROJECT_DIR=`pwd`
INSTALL_DIR=$PROJECT_DIR/out
mkdir $INSTALL_DIR
mkdir build; cd build

#build checksums
git clone https://github.com/awslabs/aws-checksums.git
cd aws-checksums && mkdir checksums-build && cd checksums-build
cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DCMAKE_BUILD_TYPE=Debug ..
make && make test && make install
cd ..

#build aws-c-common
git clone https://github.com/awslabs/aws-c-common.git
cd aws-c-common && mkdir common-build && cd common-build
cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DCMAKE_BUILD_TYPE=Debug ..
make && make test && make install
cd ..

#build aws-c-event-stream
cd $PROJECT_DIR/build
cmake -DCMAKE_PREFIX_PATH=$INSTALL_DIR -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR $PROJECT_DIR ..
make && make test && make install
