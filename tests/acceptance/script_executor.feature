Feature: Chef scripts on events


    Scenario: Successfull scripts
      Given I have configured role in farm
        And I add chef scripts to HostInit event
       When I start server
       Then I see that chef scripts were successfully executed
        And I see results of chef scritps

    Scenario: Timeouted scripts
      Given I have configured role in farm
        And I add chef scripts with small timeout to HostInit event
       When I start server
       Then I see that chef scripts failed with timeout

    Scenario: Failed scripts
      Given I have configured role in farm
        And I add broken chef scripts to HostInit event
       When I start server
       Then I see that chef scripts failed

    """
    Scenario: Chef script continue after restart
      Given I have configured role in farm
        And I add chef scripts to HostInit event
       When I start server
    """