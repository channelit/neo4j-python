#!/bin/bash

docker run \
    --publish=7474:7474 --publish=7687:7687 \
    --volume=$HOME/neo4j/data:/data \
    --volume=$HOME/neo4j/logs:/logs \
    -e NEO4J_dbms_memory_heap_max__size=3096M \
    neo4j