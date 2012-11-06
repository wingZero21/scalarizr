Feature: lvm storage

    Scenario: Extend volume when new disks added
        Given I have LVM layout on top of loop device
         When I extend pvs with another loop
         Then I see volume growth