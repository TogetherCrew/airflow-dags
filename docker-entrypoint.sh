#!/usr/bin/env bash
echo "chang dir to dags"
cd dags || exit

python3 -m coverage run --omit=tests/* -m pytest tests

cp .coverage ../.coverage
python3 -m coverage lcov -i -o coverage/lcov.info