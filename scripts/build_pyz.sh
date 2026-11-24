#!/usr/bin/env bash
# Build dist/mongo-survey.pyz: a single file that runs with `python3 mongo-survey.pyz ...`
# and needs nothing installed. It bundles mongo-survey plus pure-Python builds of
# pymongo and dnspython (pymongo's C extensions are optional; it falls back to
# pure Python when they are absent, which is slower but fine for this workload).
set -euo pipefail
cd "$(dirname "$0")/.."
BUILD=$(mktemp -d)
trap 'rm -rf "$BUILD"' EXIT
python3 -m pip install --quiet --no-compile --target "$BUILD" --no-binary pymongo .
# drop compiled extensions so the archive is platform independent
find "$BUILD" \( -name '*.so' -o -name '*.pyd' -o -name '__pycache__' \) -prune -exec rm -rf {} +
rm -rf "$BUILD"/bin "$BUILD"/*.dist-info/RECORD
mkdir -p dist
python3 -m zipapp "$BUILD" -m "mongo_survey.cli:main" -p "/usr/bin/env python3" -o dist/mongo-survey.pyz
( cd dist && shasum -a 256 mongo-survey.pyz > mongo-survey.pyz.sha256 )
ls -l dist/mongo-survey.pyz
cat dist/mongo-survey.pyz.sha256
