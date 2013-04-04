Using step definitions from: haproxy.py
Feature: Haproxy

Scenario: Proxy to a single server
	Given I have haproxy running
	Given I have launched 1 server
	When I add proxy
	I expect traffic to be proxified

Scenario: Proxy to a role
	Given I have haproxy running
	Given I have launched a role with 1 server
	When I add proxy
	I expect traffic to be proxified

Scenario: Proxy to multiple servers and roles
	Given I have haproxy running
	Given I have launched 2 servers
	Given I have launched a role with 2 servers
	Given I have launched a role with 1 server
	When I add proxy
	I expect traffic to be proxified

Scenario: Proxy to multiple servers and roles including down or backup
	Given I have haproxy running
	Given I have launched 2 servers
	Given I have launched a role with 2 servers
	Given I have launched a role with 1 server
	Given I configure some servers and roles to be down or backup
	When I add proxy
	I expect traffic to be proxified  # correctly

Scenario: Default healthcheck params
	Given I have haproxy running
	And I have prepared healthcheck configuration: {"check_interval": "5s", ...}
	Given I have launched 1 server
	When I add proxy
	I expect traffic to be proxified
	When I check health
	I expect OK
	Then I shut the servers down
	When I check health
	I expect FAIL
	# Check that healtcheck params in the conf meet our expectations?

Scenario: Diverse healthcheck params
	...
	One server with one healthcheck configuration
	Second with another
	...

Scenario: Scalr communication
	Placeholder




