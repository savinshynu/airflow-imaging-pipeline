#!/bin/bash

# command line arguments
INPUT=$1

# Check if the CASA MS file exists, and it would be a directory

if [ -d "$INPUT" ]; then
    echo "Success"
else
    echo "Failure"
fi
