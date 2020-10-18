from flask import request
import json
import pandas as pd
from pipedrive.client import Client
import requests
from datetime import datetime, time, timedelta
import pytz
import urllib
from urllib.parse import urlencode, quote


#Medilink con plataforma sandbox
key_pipedrive = 'c33d29b3de7c8421a435c3838b9d5b5b5d3b7cc7'
key_medilink = 'l6lzgzzhXaWEKzgddUIzyJLHFHcFxxCyNOHBuwV9.ISrSwKo4MwsqVrTOfAWQAsW6SY6W5DuGjbc8AhEF'
medilink_url =  "https://api.medilink.healthatom.com/api/v1/"
client = Client(domain='https://clinicatempora.pipedrive.com/')

def update_persona(request):
    request_json = request.get_json()
    person_id = request_json['current']['id']
    client.set_api_token (key_pipedrive)
    id_paciente= extraer_pacienteid_persona(person_id)
    #get datos persona pipedrive
    first_name, last_name, fecha_nacimiento, telephone, mail, cel, prevision, rut, ciudad, comuna, sexo, calle = get_datos_persona(person_id)
    #update
    mensaje = update_paciente(id_paciente, first_name, last_name, mail, rut, fecha_nacimiento, telephone, cel, sexo, ciudad, comuna, calle)
    print(mensaje)
    return ('persona updated', 200)

###### helper functions  ########


def extraer_pacienteid_persona(person_id):
    person = client.persons.get_person(str(person_id))
    id_paciente = person['data']['6c9143567d727245df03896004ab996922b581b3']
    return id_paciente

def obtener_sexo(sexo_id, client):
    """ Obtenemos el identificador del sexo desde pipedirve y buscamos su string correspondiente
    para enviarlo como input de la creacion del paciente en medilink """
    fields = client.persons.get_person_fields()
    data = fields['data']
    for j in data:
        if j['id']== 9088:
            opciones = j['options']
            for op in opciones:
                if op['id']== int(sexo_id):
                    sex = [op['label']]
                else:
                    pass
        else:
            pass
    string_sexo=sex[0]
    transform_string = string_sexo[:1]
    return transform_string

def get_datos_persona(person_id):
    person = client.persons.get_person(str(person_id))
    person_name = person['data']['name']
    first_name = person['data']['first_name']
    last_name = person['data']['last_name']
    fecha_nacimiento = person['data']['c1b4a3c5433f4dcdd28338fa4a09944c8d8b6105']
    telephone = person['data']['phone'][0]['value']
    mail = person['data']['email'][0]['value']
    cel=person['data']['ac8de99887d70232fed009b9705a879d6d2ffa5c']
    prevision=person['data']['5b10682582f50937ac24ad0da10da57c88d21d74']
    rut=person['data']['5df3dc703fa5f7dae88df356d3542c794cb32295']
    ciudad = person['data']['5a3cad7df7a570a6a783c1172961d82da9c7a653_admin_area_level_2']
    comuna = person['data']['5a3cad7df7a570a6a783c1172961d82da9c7a653_locality']
    calle = person['data']['5a3cad7df7a570a6a783c1172961d82da9c7a653']
    sexo_id= person['data']['f18049fa9810fac7cba5bfb3c3deb6289a17a87a']
    sexo = obtener_sexo(sexo_id, client)

    return first_name, last_name, fecha_nacimiento, telephone, mail, cel, prevision, rut, ciudad, comuna, sexo, calle

def update_paciente(id_paciente, first_name, last_name, mail, rut, fecha_nacimiento, telephone, cel, sexo, ciudad, comuna, calle):
    """ Agregamos paciente en medilink con los datos de la persona en Pipedrive """
    url_paciente= medilink_url + "pacientes/" + str(id_paciente)
    payload = {
            'nombre': first_name,
            'apellidos' : last_name,
            'email': mail,
            'rut': rut,
            "fecha_nacimiento" : fecha_nacimiento,
            'celular': telephone,
            'direccion': calle,
            'ciudad': ciudad,
            'comuna':comuna,
            'sexo':sexo
        }
    headers = {
        'Authorization': 'Token ' + key_medilink
    }
    response = requests.put(url_paciente, headers=headers, data = payload)
    response_json = response.json()
    return ('updated', 200)