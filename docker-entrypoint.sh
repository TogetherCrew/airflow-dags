#!/usr/bin/env bash
echo "Running tests"
cd dags || exit
echo "After cd dags"
python3 -m coverage run --omit=tests/* -m pytest tests
python3 -m coverage lcov -i -o coverage/lcov.info