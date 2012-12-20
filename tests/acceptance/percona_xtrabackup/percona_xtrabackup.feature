Feature: Percona xtrabackup

	Scenario: Create full backup
		Given i have running Percona Server
		When i create full xtrabackup
		Then i have a restore object R1

	Scenario: Create incremental backup
		Given i have running Percona Server
		And add some data
		When i create incremental xtrabackup
		Then i have a restore object R2

#	Scenario: Restore full backup
#		Given i have stopped Percona Server
#		When i restore full backup R1
#		Then i have operational Percona Server
#
#	Scenario: Restore incremental backup
#		Given i have stopped Percona Server
#		When i restore incremental backup R2
#		Then i have operational Percona Server
#		And some data from incremental backup
#