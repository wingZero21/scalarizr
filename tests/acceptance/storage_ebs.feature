Feature: EBS storage

	Scenario: Ensure new volume
		Given i create volume with size=1
		When i ensure volume
		Then it should be available in operation system
		And let it V1
		
	Scenario: Ensure existed volume
		Given i have existed EBS
		And i create volume with it's id
		When i ensure volume
		Then it should has the same id
		And it should be available in operation system
		And let it V2
		
	Scenario: Ensure volume in different zone
		Given i have existed EBS in another zone
		And i create volume with it's id
		When i ensure volume
		Then it should has another id
		But it should be created from original volume snapshot
		And let it V3
		
	Scenario: Snapshot
		When i take volume V1
		Then i take snapshot (let it S1)
		And it should be created from V1 EBS volume
		
	Scenario: Ensure snapshot restore:
		Given i have EBS snapshot
		And i create volume from it
		When i ensure volume
		Then it should be created from that snapshot
