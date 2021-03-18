mkfs -t ext4 /dev/pmem1
mount -o dax /dev/pmem1 /mnt/pmem1/
