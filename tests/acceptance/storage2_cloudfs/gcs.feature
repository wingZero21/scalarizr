Using step definitions from: gcs.py
Feature: google cloud storage fs driver

   Scenario: data integrity
       When I upload test file to random bucket
       Then I can see it on remote fs
       When I download it back
       Then I see same file i uploaded before
       When I delete file on remote fs
       Then I cannot see it on remote fs