dd if=/dev/vg/first | gzip | split -a 3 -b 15M - /mnt/second/snapshot.tar. 2> /result.log
