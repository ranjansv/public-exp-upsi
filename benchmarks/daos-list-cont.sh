POOL_UUID=`dmg -i pool list|tail -1|awk '{print $1}' 2> /dev/null`
echo "Pool UUID: $POOL_UUID"

POSIX_CONT_UUID=`ps aux|grep dfuse|grep 'container.*' -o|grep -o '[0-9].*$'`
echo "POSIX_CONT_UUID: $POSIX_CONT_UUID"

echo ""
echo "List of containers"
daos pool list-cont --pool=$POOL_UUID 2> /dev/null

CONT_UUID=`daos pool list-cont --pool=$POOL_UUID 2> /dev/null|grep -v $POSIX_CONT_UUID`
echo ""
echo "CONT_UUID: $CONT_UUID"

echo ""
daos cont query --cont=$CONT_UUID --pool=$POOL_UUID 2> /dev/null
