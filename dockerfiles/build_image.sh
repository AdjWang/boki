#!/bin/bash
set -euxo pipefail

BOKI_DIR=$(realpath $(dirname $0)/..)   # */boki/
DOCKERFILE_DIR=$BOKI_DIR/dockerfiles

DOCKER_BUILDER="docker buildx"

# NO_CACHE="--no-cache"
NO_CACHE=""

function build_buildenv {
    $DOCKER_BUILDER build $NO_CACHE -t adjwang/boki-buildenv:dev \
        -f $DOCKERFILE_DIR/Dockerfile.buildenv \
        $BOKI_DIR
}

function build {
    build_buildenv
}

function push {
    echo "TODO"
}

case "${1-default}" in
build)
    build
    ;;
push)
    push
    ;;
*)
    echo "need 1 argument"
    echo "usage: build_image.sh [build|push]"
    ;;
esac
