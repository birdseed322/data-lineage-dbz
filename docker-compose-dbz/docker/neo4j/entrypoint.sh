#!/bin/bash

# Adjust the permissions of neo4j.conf
chmod 644 /var/lib/neo4j/conf/neo4j.conf

# Continue with the original entrypoint command
exec "$@"
