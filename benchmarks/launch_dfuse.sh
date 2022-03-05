#!/bin/bash
MOUNTPOINT=$1
POOL_UUID=$2
CONT_UUID=$3

mkdir -p $MOUNTPOINT
dfuse --mountpoint=$MOUNTPOINT --pool=$POOL_UUID --container=$CONT_UUID --disable-caching &
