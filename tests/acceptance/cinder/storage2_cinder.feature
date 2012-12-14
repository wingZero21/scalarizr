Feature: Cinder storage

	Scenario: Ensure not created volume 
		Given I create CinderVolume object on server bd68804e-9083-470b-a9e2-8f2feebcdf17
		When I run ensure
		Then actual volume should be created

	Scenario: Destroy volume
		Given I have created CinderVolume object on server bd68804e-9083-470b-a9e2-8f2feebcdf17
		When I run destroy
		Then it should delete volume on cinder
		And set id attribute to None

	Scenario: Ensure volume when it already created
		Given I have created CinderVolume object on server bd68804e-9083-470b-a9e2-8f2feebcdf17
		And I save its state
		When I run ensure
		Then object should left unchanged

	Scenario: Ensure volume when it detached from server
		Given I have created CinderVolume object on server bd68804e-9083-470b-a9e2-8f2feebcdf17
		But without actual attachment
		When I run ensure
		Then volume should be attached to server 

	Scenario: Ensure volume when it located in another availability zone
		Given I have created CinderVolume object on server bd68804e-9083-470b-a9e2-8f2feebcdf17
		But it located in other availability zone
		When I run ensure
		Then volume should be moved to given zone

	Scenario: Ensure volume after changing size attribute
		Given I have created CinderVolume object on server bd68804e-9083-470b-a9e2-8f2feebcdf17
		And I set it different size
		When I run ensure
		Then CinderVolume should recover its true size

	Scenario: Snapshot a volume
		Given I have created CinderVolume object on server bd68804e-9083-470b-a9e2-8f2feebcdf17
		When I run create snapshot
		Then actual snapshot should be created

	