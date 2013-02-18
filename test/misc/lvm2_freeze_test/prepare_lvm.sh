#!/bin/sh
apt-get -y install lvm2 sysbench
losetup -f 
df -h 

dd if=/dev/urandom of=/test.img bs=1024 count=7000000 
losetup /dev/loop1 /test.img 

pvcreate /dev/loop1 
vgcreate vg /dev/loop1 
vgs 

uname -a 
modprobe dm_mod 

lvcreate -n first -L 2000M vg 
lvcreate -n second -L 2000M vg 

mkdir /mnt/first 
mkdir /mnt/second
mkfs -t ext3 -m 1 -v /dev/vg/first 
mkfs -t ext3 -m 1 -v /dev/vg/second 
mount -t ext3 /dev/vg/first /mnt/first/ 
mount -t ext3 /dev/vg/second /mnt/second/  
ls -la /mnt/first/ 
ls -la /mnt/second/ 
df -h 

lvcreate -L 2000M -s -n backup /dev/vg/first 
lvs 
vgs 
mkdir /mnt/backup/ 
mount /dev/vg/backup /mnt/backup 

cd /mnt/first/ && sysbench --test=fileio --file-total-size=1900M --file-test-mode=rndrw prepare
