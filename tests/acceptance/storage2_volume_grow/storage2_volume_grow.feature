Feature: Storage2 volume grow


	Scenario Outline: grow mounted volume
		Given I have <type> volume with <cfg> settings
		And I create some file on it
		When I grow volume with <grow_cfg>
		Then I see that volume size increased properly
		And I still see my precious file

	Examples:
		| type | cfg                     | grow_cfg |
		| loop | size=0.05, fstype=ext3  | size=0.1 |
		| loop | size=0.05, fstype=ext4  | size=0.1 |
		| loop | size=0.05, fstype=xfs   | size=0.1 |
		| ebs  | size=1, fstype=ext3   	 | size=2   |
		| ebs  | size=1, fstype=ext4     | size=2   |
		| ebs  | size=1, fstype=xfs      | size=2   |


	Scenario Outline: grow mounted array
		Given I have raid <volume>
   		And I create some file on it
   		When I <grow> raid volume
   		Then I see that raid grew properly
   		And I still see my precious file

	Examples:
		| volume															| grow 				      |
		| vg=test,level=5,disks=3,disk.type=loop,disk.size=0.05,fstype=ext3	| foreach.size=0.1        |
		| vg=test,level=5,disks=3,disk.type=loop,disk.size=0.05,fstype=ext4	| foreach.size=0.1        |
		| vg=test,level=5,disks=3,disk.type=loop,disk.size=0.05,fstype=xfs	| foreach.size=0.1        |

		| vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=ext3	| foreach.size=0.1,len=3  |
	    | vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=ext4	| foreach.size=0.1,len=3  |
    	| vg=test,level=5,disks=2,disk.type=loop,disk.size=0.05,fstype=xfs	| foreach.size=0.1,len=3  |

		| vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=ext3 | foreach.size=0.1,len=3  |
		| vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=ext4 | foreach.size=0.1,len=3  |
		| vg=test,level=1,disks=2,disk.type=loop,disk.size=0.05,fstype=xfs  | foreach.size=0.1,len=3  |
