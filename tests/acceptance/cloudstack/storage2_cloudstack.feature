Feature: Cloudstack storage

    Scenario: Ensure not created volume 
        Given I create CSVolume object on server 2b732502-6f6d-4c34-90e1-ed1bf94893ec
        When I run ensure
        Then actual volume should be created

    Scenario: Destroy volume
        Given I have created CSVolume object on server 2b732502-6f6d-4c34-90e1-ed1bf94893ec
        When I run destroy
        Then it should delete volume on cloudstack
        And set id attribute to None

    Scenario: Ensure volume when it already created
        Given I have created CSVolume object on server 2b732502-6f6d-4c34-90e1-ed1bf94893ec
        And I save its state
        When I run ensure
        Then object should left unchanged

    Scenario: Ensure volume when it detached from server
        Given I have created CSVolume object on server 2b732502-6f6d-4c34-90e1-ed1bf94893ec
        But without actual attachment
        When I run ensure
        Then volume should be attached to server 

    #Scenario: Ensure volume when it located in another availability zone
    #    Given I have created CSVolume object on server 2b732502-6f6d-4c34-90e1-ed1bf94893ec
    #    But it located in other availability zone
    #    When I run ensure
    #    Then volume should be moved to given zone

    Scenario: Ensure volume after changing size attribute
        Given I have created CSVolume object on server 2b732502-6f6d-4c34-90e1-ed1bf94893ec
        With wrong size
        When I run ensure
        Then CSVolume should recover its true size

    Scenario: Snapshot a volume
        Given I have created CSVolume object on server 2b732502-6f6d-4c34-90e1-ed1bf94893ec
        When I run create snapshot
        Then actual snapshot should be created
