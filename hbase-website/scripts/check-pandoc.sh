#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Check if Pandoc is installed and provide installation instructions

set -e

echo "=================================="
echo "Pandoc Installation Check"
echo "=================================="
echo ""

if command -v pandoc &> /dev/null; then
    VERSION=$(pandoc --version | head -n 1)
    echo "✓ Pandoc is installed: $VERSION"
    echo ""
    echo "You're ready to convert documentation!"
    echo "Run: npm run convert:adoc"
    exit 0
else
    echo "✗ Pandoc is not installed"
    echo ""
    echo "Please install Pandoc to convert documentation:"
    echo ""
    
    # Detect OS and provide specific instructions
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "  macOS (using Homebrew):"
        echo "    brew install pandoc"
        echo ""
        echo "  macOS (using MacPorts):"
        echo "    sudo port install pandoc"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "  Ubuntu/Debian:"
        echo "    sudo apt-get update"
        echo "    sudo apt-get install pandoc"
        echo ""
        echo "  Fedora/CentOS:"
        echo "    sudo dnf install pandoc"
        echo ""
        echo "  Arch Linux:"
        echo "    sudo pacman -S pandoc"
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
        echo "  Windows (using Chocolatey):"
        echo "    choco install pandoc"
        echo ""
        echo "  Windows (using Scoop):"
        echo "    scoop install pandoc"
    fi
    
    echo ""
    echo "  Or download from: https://pandoc.org/installing.html"
    echo ""
    exit 1
fi


