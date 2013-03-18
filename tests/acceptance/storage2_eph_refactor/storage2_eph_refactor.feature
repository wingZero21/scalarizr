Feature: migrate from old eph storage2 to new (without adapter)
	In order to create ephemeral volumes
	I want storage2 to understand old-style configurations

	Scenario: from existing config
		When I create eph volume from existing old-style config
        Then I see lvm layer was created

	Scenario: from empty config
		When I create eph volume from empty old-style config
         And I see lvm layer was created

	Scenario: from snapshot
		When I create eph volume from snapshot old-style config
		 And I see lvm layer was created
	 	 And I see snapshotted file
