#!/usr/bin/env bash

set -e

version="v3"

. venv/bin/activate
pyinstaller -F -y youtic.spec

rm -f dist/*.log
rm -f dist/*.csv
rm -f dist/.DS_Store

zip -r youtic-${version}.zip dist
git archive master -o youtic-src-${version}.zip
