# Integration Pipedrive and Medilink

This project is the source code for a google cloud integration between Pipedrive CRM and health software Medilink. It connects both software API's punlicly available:

Pipedrive API: https://developers.pipedrive.com/docs/api/v1/#/
Medilink API: https://developers.pipedrive.com/docs/api/v1/

The software is a function with dependent helper functions that fundamentally receives webhooks from the Pipedrive CRM and acta accordingly several protocols. The webhook in Pipedrive is triggered by updating a deal and has effects on the following updates:

- Change in stage of the pipeline
- Change of key fields

With 3 cases of webhooks where certain conditions are met, the function performs operations on the Medilink software such as creating patients, deleting dates and date states.
Basic architecture is outlines below.

