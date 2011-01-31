#!/bin/sh
cd /mnt/first/ && sysbench --test=fileio --file-total-size=1900M --file-test-mode=rndrw cleanup

umount /mnt/backup && rmdir /mnt/backup 
lvremove -f /dev/vg/backup 

cd / && umount /mnt/first && rmdir /mnt/first 
umount /mnt/second && rmdir /mnt/second 
lvremove -f /dev/vg/first 
lvremove -f /dev/vg/second 

vgreduce vg /dev/loop1 
pvremove -f /dev/loop1 

losetup -d /dev/loop1 

#rm -f /test.img
