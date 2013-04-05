Using step definitions from: nginx.py
Feature: Nginx

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
    Then server appears in backend

Scenario: Proxy to a role, and server goes down
    Given i have a proxy to a role
    When i terminate one server of this role
    Then server removed from backend

Scenario: Proxy HTTPS
    Given i have a server
    And i have SSL keypair
    When i add proxy
    Then i expect proxying
    Then i expect proxying https -> http


Scenario HTTPS only
    Given i have a server
    And i have HTTP disabled
    When i add proxy
    Then i expect proxying
    And i expect redirect https -> http

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
    And i have i backup server SB
    And i have a regular role R
    And i have a backup role RB
    And i have a down role RD
    When i add proxy
    Then i expect S and R servers are regular in backend
    And i expect SD and RD servers are down in backend
    And i expect SB and RB servers are backup in backend 
