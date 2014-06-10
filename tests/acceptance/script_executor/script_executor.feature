Feature: Chef scripts on events


    Scenario: Successfull scripts
      Given I have configured role in farm
        And I add chef scripts to HostInit event
       When I start server
       Then I see that chef scripts were successfully executed
