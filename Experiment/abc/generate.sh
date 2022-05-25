#!/bin/sh

export PYTHONPATH=/usr/lib/python3.8/dist-packages:/usr/lib/pegasus/externals/python

#### Executing Workflow Generator ####
./diamond-workflow/workflow_generator.py -s -e condorpool -o workflow.yml
#### Generating Pegasus Properties ####
echo "pegasus.transfer.arguments = -m 1" > pegasus.properties
#### Generating Sites Catalog ####
python3 /home/sai/.pegasus/pegasushub/5.0/Sites.py \
    --execution-site CONDORPOOL \
    --project-name "" \
    --queue-name "" \
    --pegasus-home "" \
    --scratch-parent-dir /home/sai/Desktop/ISI/Genetic Algorithms/Experiment/abc \
    --storage-parent-dir /home/sai/Desktop/ISI/Genetic Algorithms/Experiment/abc
