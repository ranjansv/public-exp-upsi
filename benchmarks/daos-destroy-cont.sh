#!/bin/bash
daos pool list-cont --pool=$POOL_UUID |sed -e '1,2d'|awk '{print $1}'|xargs -L 1 -I '{}' sh -c "daos cont destroy --cont={} --pool=$POOL_UUID --force"
