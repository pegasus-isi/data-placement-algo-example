#!/bin/sh

pegasus-plan --conf pegasus.properties \
    --dir submit \
    --sites condorpool \
    --output-sites local \
    --cleanup leaf \
    --force \
    workflow.yml
