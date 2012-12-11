Feature: Percona xtrabackup

	Scenario: Create streaming backup to S3
		Given i have running Percona server
		When i create full xtrabackup
		Then i have a restore object
		And cloudfs_src points to valid manifest
