Feature: Storage2 raid disk replace

	Scenario Outline: Replace disk 

		Given I have <type1> volume with <cfg1> settings
        When I replace disk 0 with disk <type2> with <cfg2> settings
        Then I see that disk 0 was replaced

	Examples:
		| type1 | cfg1                                                               | type2 | cfg2
											
		| raid  | vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=ext3  | loop  | size=0.05, fstype=ext3
		| raid  | vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=ext4  | loop  | size=0.05, fstype=ext4
		| raid  | vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=xfs   | loop  | size=0.05, fstype=xfs

		| raid  | vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=ext3  | loop  | size=0.05, fstype=ext3
		| raid  | vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=ext4  | loop  | size=0.05, fstype=ext4
		| raid  | vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=xfs   | loop  | size=0.05, fstype=xfs

		| raid  | vg=test,level=10,disks=2,disk.type=loop,disk.size=0.05,fstype=ext3 | loop  | size=0.05, fstype=ext3
		| raid  | vg=test,level=10,disks=2,disk.type=loop,disk.size=0.05,fstype=ext4 | loop  | size=0.05, fstype=ext4
		| raid  | vg=test,level=10,disks=2,disk.type=loop,disk.size=0.05,fstype=xfs  | loop  | size=0.05, fstype=xfs
                                                                                                                    
