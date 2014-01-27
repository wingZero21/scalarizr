Using step definitions from: block_device.py
Feature: Additional storage for scalr servers

    Scenario: Create and attach additional storage
        Given I have configured role with additional storage:
            """
            { "type": "habibi", "size": 1, "mpoint": "/mnt/mystorage" }
            """
        When I start farm
         And I scale my role to 2 servers
        Then I see 2 running servers
         And I see additional storages were created and attached
         And I create some unique data on these storages

    Scenario: Farm restart
        When I restart my farm
        Then I see 2 running servers
         And I see that old storages were attached
         And I see my unique data on those storages
