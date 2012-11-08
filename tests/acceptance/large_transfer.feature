Feature: Large transfer

Scenario: Upload single file
	Given I have a big file F1
	When I upload it with gzipping
	Then I expect manifest as a result
	And all chunks are uploaded 

Scenario: Upload single dir


Scenario: Upload files and dirs


Scenario: Download file when one or several chunks are missed
