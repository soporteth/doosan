# Integración Medilink y Pipedrive

La siguiente integración se realiza con el objetivo de utilizar los webhooks de Pipedrive para generar cambios dentro del software de medilink. Para el desarrollo de este proyecto se utilizaron las siguiente tecnologías:

- Medilink (https://www.softwaremedilink.com/)
- Pipedrive CRM (https://www.pipedrive.com/es)
- GCP Google Cloud Functions (https://cloud.google.com/functions?hl=es)

La documentación de las API de ambos software se pueden encontrar en los siguientes links:
- Pipedrive: https://developers.pipedrive.com/docs/api/v1/
- Medilink: https://api.medilink.healthatom.com/docs/?Javascript#inicio

Esta documentación incluye el codigo para ambas funcione utilizadas.
1. funcion_integracion: Se encarga de realizar el "core" de la integración. Esta funcion es la que recibe la información de todo lo que pasa en los tratos. Toma en cuenta los cambios de etapas del trato, los cambios en los campos relevantes y es la que computa la mayor cantidad de acciones. 
2. persona_update: Se encarga de detectar cambios en los campos de la Persona en Pipedrive. Pone al día en Medilnk la información de los pacientes si es que estos son editados en Pipedrive.

