#!/bin/bash

echo "check_regular = True"
echo "test_params = $1"

conda activate forecast
python AnalysisPipeLine.py check_regular
