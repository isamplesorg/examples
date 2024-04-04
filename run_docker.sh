#!/bin/sh

set -e
docker build --no-cache -f Dockerfile -t rdhyee/isamples-examples .
PORT=${1:-8888}

# export EZID_USER=op://OpenContext/EZID-for-OpenContext/username
# export EZID_PASSWD=op://OpenContext/EZID-for-OpenContext/credential

docker run \
   -v "${PWD}":/home/jovyan/work \
   -p $PORT:8888 \
   -e GEN_CERT=yes \
   -e GRANT_SUDO=yes \
   -e EZID_USER=$EZID_USER \
   -e EZID_PASSWD=$EZID_PASSWD \
   rdhyee/isamples-examples \
   start-notebook.sh \
   --NotebookApp.password='argon2:$argon2id$v=19$m=10240,t=10,p=8$qvckJKX8B1thQjA0CYmw/Q$v8nHdCbdSZPfxWCVU7bIhI0w4/GjWZuNsrw8AkhWXdo'
