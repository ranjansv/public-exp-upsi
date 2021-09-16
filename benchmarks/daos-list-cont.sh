echo "Pool UUID: $POOL_UUID"

echo ""
echo "List of containers"
daos pool list-cont --pool=$POOL_UUID

CONT_UUID=`daos pool list-cont --pool=$POOL_UUID | tail -1 | awk '{print $1}'`
echo ""
echo "CONT_UUID: $CONT_UUID"

echo ""
daos cont query --cont=$CONT_UUID --pool=$POOL_UUID 
