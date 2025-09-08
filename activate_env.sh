#!/bin/bash
# Activation script for Azure Databricks environment
# Run this script to activate the azure_databricks conda environment

echo "Activating Azure Databricks environment..."
conda activate azure_databricks

if [ $? -eq 0 ]; then
    echo "✅ Azure Databricks environment activated successfully!"
    echo "Python version: $(python --version)"
    echo "Python path: $(which python)"
    echo ""
    echo "Available Databricks packages:"
    pip list | grep -i databricks
else
    echo "❌ Failed to activate azure_databricks environment"
    echo "Please ensure the environment exists: conda env list"
fi

