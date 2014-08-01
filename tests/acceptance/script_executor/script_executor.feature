Feature: Chef scripts on events


    Scenario: Successfull chef-solo scripts
      Given I have configured role in farm
        And I add chef-solo scripts to HostInit event
       When I start server
       Then I see that chef scripts were successfully executed

    @client
    Scenario: Successfull chef-client scripts
      Given I have configured role in farm
        And I have configured chef-client for the role
       When I start server
       Then I see that chef scripts were successfully executed

