#!/bin/bash
# Get a list of staged files
staged_files=$(git diff --cached --name-only | grep -v "lint.sh" | grep -vE "\.db" | grep -vE "\.pickle$" | grep -vE "\.html$" | grep -v "\.json" | grep -vE "\.txt$" | grep -vE "\.yaml$" | grep -vE "\.csv$" | grep "csv_pipeline/")

# Get a list of files in the csv_pipeline folder
csv_pipeline_files=$(git ls-files "csv_pipeline/")

# Take the intersection of staged_files and csv_pipeline_files
intersection_files=$(comm -12 <(echo "$staged_files" | sort) <(echo "$csv_pipeline_files" | sort))

# Check if there are intersection files
if [ -z $intersection_files ]; then
  echo "No intersection staged files to process."
  exit 0
fi

echo "Running isort"
isort $intersection_files
echo "Running autoflake"
autoflake --remove-all-unused-imports --recursive --in-place --ignore-init-module-imports $intersection_files
echo "Running black"
black -t py311 --fast --line-length=120 $intersection_files
echo "Running autopep8"
autopep8 --in-place --recursive $intersection_files
echo "Running flake"
flake8 $intersection_files --statistics --ignore=E501,E402,W503,E203
