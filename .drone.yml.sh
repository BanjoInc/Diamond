#!/bin/bash
cat << EOF > .drone.yml
build:
  # Step 1 - build docker image!
  docker-image:
    image: docker:latest
    volumes:
      # Expose the host docker daemon socket to this container.. DinD
      - /var/run/docker.sock:/var/run/docker.sock
    commands:
      - docker build -t $DOCKER_IMAGE_TAG .
  # Step 2
  make-stuff:
    image: $DOCKER_IMAGE_TAG
    commands:
      - $USER_COMMAND
      - $USER_SU
      - make rpm
publish:
  s3:
    acl: public-read
    region: $DRONE_S3_REGION
    bucket: $DRONE_S3_BUCKET
    access_key: $DRONE_S3_ACCESS_KEY
    secret_key: $DRONE_S3_SECRET_KEY
    source: $DRONE_S3_SOURCE
    target: $DRONE_S3_TARGET
    recursive: true
EOF
