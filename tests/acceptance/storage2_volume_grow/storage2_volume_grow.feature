Feature: Storage2 volume grow

	Scenario Outline: grow mounted volume
		Given I have <type> volume with "<cfg>" settings
		And I create some file on it
		When I grow volume with "<grow_cfg>"
		Then I see that volume size increased properly
		And I still se my precious file

	Examples:
		| type | cfg                   | grow_cfg |
		| loop | size=0.1, fstype=ext3 | size=0.5 |
		| loop | size=0.1, fstype=ext4 | size=0.5 |
		| loop | size=0.1, fstype=xfs  | size=0.5 |
		| ebs  | size=1, fstype=ext3   | size=2   |
		| ebs  | size=1, fstype=ext4   | size=2   |
		| ebs  | size=1, fstype=xfs    | size=2   |