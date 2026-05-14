#!/bin/bash

echo "Cleaning Python cache..."
find . -type d -name "__pycache__" -exec rm -r {} +

echo "Cleaning .pyc..."
find . -name "*.pyc" -delete

echo "Cleaning .DS_Store..."
find . -name ".DS_Store" -delete

echo "Done."