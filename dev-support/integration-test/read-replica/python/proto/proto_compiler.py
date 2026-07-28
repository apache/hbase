#!/usr/bin/env python3
"""
Compiles all *.proto files in the 'python/proto' directory. The compiled output is sent to 'python/proto/generated'.
This script removes any existing 'generated' directory and creates a new one every time.
"""
import os
from grpc_tools import protoc
from python.src.logger_config import get_logger

logger = get_logger(__name__)


if __name__ == '__main__':
    proto_dir = os.path.dirname(__file__)
    generated_dir = os.path.join(proto_dir, 'generated')

    if os.path.exists(generated_dir):
        os.rmdir(generated_dir)
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
