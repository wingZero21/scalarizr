Using step definitions from: haproxy.py
Feature: Haproxy

Scenario: Proxy to a server
    Given i have a server  
    When i add proxy
    Then i expect proxying

Scenario: Proxy to a role
    Given i have a role
    When I add proxy
    Then i expect proxying

Scenario: Proxy to a role, and new server up
    Given i have a proxy to a role
    When i launch new server of this role
    Then server appears in the backend

Scenario: Proxy to a role, and server goes down
    Given i have a proxy to a role
    When i terminate one server of this role
    Then server is removed from the backend

Scenario: Proxy to backup role
    Given i have a proxy to two roles: master and backup
    When i terminate master servers
    Then i expect proxying to backup servers

# later
Scenario: Mark server as down
    Given i have a proxy to two servers
    When i update proxy marking one server as down
    Then i expect proxying to remaining server

Scenario: Proxy with advancend configuration
    Given i have a regular server S
    And i have a down server SD
    And i have a backup server SB
    And i have a regular role R
    And i have a backup role RB
    And i have a down role RD
    When i add proxy
    Then i expect S and R servers are regular in the backend
    And i expect SD and RD servers are down in the backend
    And i expect SB and RB servers are backup in the backend 

Scenario: Healthcheck application
    Given i have a role R1
    And i have a server S1
    And i have a role R2 with custom healthcheck C1
    And i have a server S2 with custom healthcheck C2
    When i add proxy
    Then i expect server S1 and R1 servers having default healthcheck
    And i expect server S2 having custom healthcheck C1
    And i expect R2 servers having custom healthcheck C2

Scenario: Get health
    Given i have a proxy P1 to a running servers
    And i have a proxy P2 to a broken servers
    When i get health
    Then i expect P1 servers are OK
    And i expect P2 servers are FAIL
    And i expect fail explanation





