#!/usr/bin/env bash
set -euo pipefail

pip install --upgrade pip
pip install keyrings.alt
pip install -e '.[test]'
