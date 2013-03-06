Feature: Redis password reset

    Scenario: Password is changed
        Given I am connected to Redis server
        When I call reset password
        Then password should be changed

# How to implement this?
#    Scenario: Replication works when password is changed
#        Given I have working master and slave
#        When I change password on master
#        Then replication should keep working
#
#    Scenario: Data bundle works when password is changed
#        Given I have working master and slave
#        When I change password on master
#        Then data bundle should work
#
#    Scenario: Backup works when password is changed
#        Given I have working master and slave
#        When I change password on master
#        Then backup should work
