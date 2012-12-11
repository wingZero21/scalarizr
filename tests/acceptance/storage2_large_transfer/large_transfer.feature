Feature: Large transfer

Scenario: Upload single file
    Initialize upload variables
	Given I have a 10 megabytes file F1
	When I upload it to s3 with gzipping
	Then I expect manifest as a result
	And all chunks are uploaded

Scenario: Download single file
    Given I have info from the previous upload
    When I download with the manifest
    Then I expect original items downloaded

Scenario: Upload single dir
    Initialize upload variables
    Given I have a dir D/ with 10 megabytes file F1, with 10 megabytes file F2
    When I upload it to s3 with gzipping
    Then I expect manifest as a result
    And all chunks are uploaded

Scenario: Download single dir
    Given I have info from the previous upload
    When I download with the manifest
    Then I expect original items downloaded

Scenario: Upload files and dirs
    Initialize upload variables
    Given I have a 10 megabytes file F1
    And I have a 10 megabytes file F2
    Given I have a dir D1 with 10 megabytes file F1, with 10 megabytes file F2
    And I have a dir D2 with 10 megabytes file F1, with 10 megabytes file F2
    When I upload multiple sources to s3 with gzipping
    Then I expect manifest as a result
    And all chunks are uploaded

Scenario: Download files and dirs
    Given I have info from the previous upload
    When I download with the manifest
    Then I expect original items downloaded

Scenario: Upload list of streams
    Initialize upload variables
    Given I have a list with 10 megabytes stream S1, with 10 megabytes stream S2
    When I upload multiple sources to s3 with gzipping
    Then I expect manifest as a result
    And all chunks are uploaded

Scenario: Download list of streams
    Given I have info from the previous upload
    When I download with the manifest
    Then I expect original items downloaded

Scenario: Compatibility with the old manifest
    Initialize upload variables
    Given I have a dir D/ with 10 megabytes file F1, with 10 megabytes file F2
    When I upload it to s3 with gzipping
    Then I expect manifest as a result
    And all chunks are uploaded
    I clear the tempdir and replace the manifest with it's old representation
    When I download with the manifest
    Then I expect original items downloaded

Scenario: Download file when one or several chunks are missing
    Initialize upload variables
    Given I have a 10 megabytes file F1
    When I upload it to s3 with gzipping
    Then I expect manifest as a result
    And all chunks are uploaded
    I delete one of the chunks
    When I download with the manifest
    Then I expect failed list returned
