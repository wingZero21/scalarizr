Feature: Percona utilities

	Scenario: Install Percona repository on demand
		Given i have no percona repository on machine
		When i execute innobackup --version
		Then it finishes with 0 code
		And i have percona repostory on machine

	Scenario: Ensure repository install only once
		Given i have percona repostory on machine
		When i execute innobackup --version
		Then it finishes with 0 code