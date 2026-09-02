#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Compiles all *.proto files in the 'python/proto' directory. The compiled output is sent to 'python/proto/generated'.
This script removes any existing 'generated' directory and creates a new one every time.
"""
import os
import shutil

from grpc_tools import protoc
from python.src.logger_config import get_logger

logger = get_logger(__name__)


if __name__ == '__main__':
    proto_dir = os.path.dirname(__file__)
    generated_dir = os.path.join(proto_dir, 'generated')

    if os.path.exists(generated_dir):
        shutil.rmtree(generated_dir)
    os.mkdir(generated_dir)

    proto_files = [file for file in os.listdir(proto_dir) if file.endswith('.proto')]
    for file in proto_files:
        logger.info(f"Compiling {file} and sending output to {generated_dir}")
        protoc.main((
            '',
            f'-I{proto_dir}',
            f'--python_out={generated_dir}/.',
            f'--pyi_out={generated_dir}/.',
            os.path.join(proto_dir, file),
        ))
