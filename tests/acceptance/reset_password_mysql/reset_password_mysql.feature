Feature: MySql password reset

    Scenario: Password is changed
        Given I am connected to MySql server
        When I call reset password
        Then password should be changed
