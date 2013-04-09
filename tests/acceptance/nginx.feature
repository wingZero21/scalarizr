Using step definitions from: nginx.py
Feature: Nginx

Scenario: Proxy to a server
    Given I have a server  
    When I add proxy
    Then I expect proxying

Scenario: Proxy to a role
    Given I have a role
    When I add proxy
    Then I expect proxying

Scenario: Proxy to a role, and new server up
    Given I have a proxy to a role
    When I launch new server of this role
    Then server appears in backend

Scenario: Proxy to a role, and server goes down
    Given I have a proxy to a role
    When I terminate one server of this role
    Then server removed from backend

Scenario: Proxy HTTPS
    Given I have a server
    And I have SSL keypair
    When I add proxy
    Then I expect proxying
    Then I expect proxying https -> http


Scenario: HTTPS only
    Given I have a server
    And I have HTTP disabled
    When I add proxy
    Then I expect proxying
    And I expect redirect https -> http

Scenario: Proxy to backup role
    Given I have a proxy to two roles: master and backup
    When I terminate master servers
    Then I expect proxying to backup servers

# later
Scenario: Mark server as down
    Given I have a proxy to two servers
    When I update proxy marking one server as down
    Then I expect proxying to remaining server

Scenario: Proxy with advancend configuration
    Given I have a regular server S
    And I have a down server SD
    And I have I backup server SB
    And I have a regular role R
    And I have a backup role RB
    And I have a down role RD
    When I add proxy
    Then I expect S and R servers are regular in backend
    And I expect SD and RD servers are down in backend
    And I expect SB and RB servers are backup in backend 
