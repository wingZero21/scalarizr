Feature: Cancel xtrabackup

Scenario: Cancel
    Given I have used the storage for 1000 MB
    And I have sent CreateDataBundle message
    When I wait for 3 seconds
    And I send CancelDataBundle message
    Then I expect it canceled
