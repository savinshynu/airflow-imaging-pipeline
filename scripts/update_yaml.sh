#!/bin/bash

# command line arguments
INPUT_MS=$1
OUT_NAME=$2
LOG_NAME=$3

LOG_DIR="/data/horse/ws/sash820h-test-corr/prod/stimela_logs/"$LOG_NAME""
OUT_DIR="/data/horse/ws/sash820h-test-corr/prod/images/"$OUT_NAME""
yq "
    .MKtest.inputs.ms_test.default = \"$INPUT_MS\" |
    .MKtest.steps.clean-image.params.prefix = \"$OUT_DIR\" |
    .opts.log.dir = \"$LOG_DIR\"
    " /data/horse/ws/sash820h-test-corr/prod/yaml/stimela-slurm.yaml > /data/horse/ws/sash820h-test-corr/prod/yaml/stimela_"$LOG_NAME".yaml

echo "/data/horse/ws/sash820h-test-corr/prod/yaml/stimela_"$LOG_NAME".yaml"