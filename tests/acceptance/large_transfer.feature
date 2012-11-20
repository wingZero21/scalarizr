Feature: Large transfer

Scenario: Upload single file
	Given I have a 10 megabytes file F1
	When I upload it to s3 with gzipping
	Then I expect manifest as a result
	And all chunks are uploaded 

Scenario: Upload single dir
    Given I have a dir D with 10 megabytes file F1, with 110 megabytes file F2
    When I upload it to s3 with gzipping
    Then I expect manifest as a result
    And all chunks are uploaded

Scenario: Upload files and dirs


Scenario: Download file when one or several chunks are missing
