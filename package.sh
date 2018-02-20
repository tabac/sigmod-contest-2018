#!/bin/bash

tar --dereference --exclude='build' --exclude='submission.tar.gz' --exclude='small_workload' -czf submission.tar.gz *
