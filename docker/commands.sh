#!/bin/bash

docker run -p 7474:7474 -p 7687:7687 -v $HOME/neo4j/data:/data -v $HOME/neo4j/logs:/logs -e NEO4J_dbms_memory_heap_max__size=3096M  --name=neo neo4j