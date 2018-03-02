#!/bin/bash

tar --dereference \
    --exclude='build' \
    --exclude='submission.tar.gz' \
    --exclude='data' \
    --exclude='docker' \
    --exclude='test' \
    --exclude='run.sh' \
    -czf \
    submission.tar.gz *
