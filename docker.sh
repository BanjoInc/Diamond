#!/bin/bash
function log {
    echo "[docker] - $1"
}

# Environment checks
printf "Using DOCKER_IMAGE=%s\n" "${DOCKER_IMAGE:?No docker image set.}"

DOCKER_HOME_DIR=/home/banjo
DOCKER_WORK_DIR=$DOCKER_HOME_DIR/docker
DOCKER_CID_FILE=/tmp/$(basename $0).$$

log "Creating container with --cidfile=$DOCKER_CID_FILE"
docker create -t --cidfile=$DOCKER_CID_FILE --workdir=$DOCKER_WORK_DIR $DOCKER_IMAGE bash || exit 1
DOCKER_CONTAINER_ID=$(cat $DOCKER_CID_FILE)

log 'Starting container'
docker start $DOCKER_CONTAINER_ID || exit 1

log 'Copying source to container'
docker cp . $DOCKER_CONTAINER_ID:$DOCKER_WORK_DIR || exit 1
docker exec -u root $DOCKER_CONTAINER_ID pip install -r .travis.requirements.txt
# Downgrade mock so tests pass..
docker exec -u root $DOCKER_CONTAINER_ID pip install 'mock<=1.0'
docker exec -u root $DOCKER_CONTAINER_ID make test
container_exit=$?
if [[ $container_exit > 0 ]]; then
  echo "EXIT CODE OF CONTAINER IS $container_exit"
  exit 1
fi
docker exec -u root $DOCKER_CONTAINER_ID make rpm

log 'Copying build artifacts out of container'
docker cp $DOCKER_CONTAINER_ID:$DOCKER_WORK_DIR/dist/ . || exit 1

log 'Stopping container'
docker stop $DOCKER_CONTAINER_ID || exit 1

log 'Cleaning up'
docker rm $DOCKER_CONTAINER_ID || exit 1
rm $DOCKER_CID_FILE || exit 1

log 'Success!'
