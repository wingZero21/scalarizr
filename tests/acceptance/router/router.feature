Feature: Router role for VPC support
    Scenario: Bootstrap router role
        When i receive HIR message
        Then i see recipe scalarizr_proxy applied
        And iptables masquerading rules applied