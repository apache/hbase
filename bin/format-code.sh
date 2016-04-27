#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'


find core connection serde utils -name "*.h" -or -name "*.cc" | xargs -P8 clang-format -i
find core connection serde utils third-party -name "BUCK" | xargs -P8 yapf -i
