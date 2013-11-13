Using step definitions from: nginx.py
Feature: Nginx

# 1
Scenario: Proxy to a server
    Given I have a server
    When I add proxy
    Then I expect proxying to server

# 2
Scenario: Proxy to a role
    Given I have a role
    When I add proxy
    Then I expect proxying to role

#3
Scenario: Proxy to a role, and new server up
    Given I have a proxy to a role
    When I launch new server of this role
    Then server appears in backend

#4
Scenario: Proxy to a role, and server goes down
    Given I have a proxy to a role
    When I terminate one server of this role
    Then server removed from backend

# 5
Scenario: Proxy HTTPS
    Given I have a server
    And I have SSL keypair
    When I add proxy
    Then I expect proxying https -> http

#6
Scenario: HTTPS only
    Given I have a server
    And I have SSL keypair
    And I have HTTP disabled
    When I add proxy
    Then I expect proxying to server
    And I expect redirect http -> https

# 7
Scenario: Proxy to backup role
    Given I have a proxy to two roles: master and backup
    When I terminate master servers
    Then I expect proxying to backup servers

# 8
Scenario: Mark server as down
    Given I have a proxy to two servers
    When I update proxy marking one server as down
    Then I expect proxying to remaining server

# 9
Scenario: Proxy with advancend configuration
    Given I have a regular server S
    And I have a down server SD
    And I have a backup server SB
    And I have a regular role R
    And I have a backup role RB
    And I have a down role RD
    When I add proxy
    Then I expect S and R servers are regular in backend
    And I expect SB and RB servers are backup in backend 
    And I expect SD and RD servers are down in backend

# 10
Scenario: Reconfigure passes
    Given I have proxy to server
    When I reconfigure proxy to another server
    Then I expect proxying to another server

# 11
Scenario: Reconfigure fails
    Given I have proxy to server
    When I reconfigure proxy with bad configuration
    Then I expect exception to be thrown
    And proxy should remain unchanged