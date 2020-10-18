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


def pipedrive_medilink(request):
    request_json = request.get_json()
    deal_id = request_json['current']['id']
    previous_stage_id = request_json['previous']['stage_id']
    stage_id =  request_json['current']['stage_id']
    person_id = request_json['current']['person_id']

    client.set_api_token (key_pipedrive)

    if stage_id == 2 and previous_stage_id == 1:
        """ Aca la funcion añade el paciente a medilink o reporta el error. En caso que ya exista, no hace nada y reporta exito en campo de Persona.
        Esta funcionalidad SOLO se activa si es que se avanza el trato de la etapa 1 a la etapa 2 """
        #enter Pipedrive credentials

        #Get Datos de la persona asociado a la actividad creada
        person = client.persons.get_person(str(person_id))

        #extraer atributos de persona
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
        sexo_id= person['data']['f18049fa9810fac7cba5bfb3c3deb6289a17a87a']
        sexo = obtener_sexo(sexo_id, client)

        try:
            paciente_id = agregar_paciente(person_id, first_name, last_name, mail, rut, fecha_nacimiento, telephone, cel, sexo, ciudad, comuna)
            update_person_pipedrive(person_id, paciente_id)
            return ('agregado/ya existia en medilink', 200)

        except:
            validacion_paciente_agregado(person_id, 'Error al agregar')
            return ('error, no se pudo agregar paciente en medilink', 400)

    if (stage_id == 3 and previous_stage_id == 2):

        #obtener datos de persona asociada al deal
        person = client.persons.get_person(str(person_id))
        person_name = person['data']['name']
        
        try:
            #Extraer ID Paciente de la persona en Pipedrive
            id_paciente = extraer_pacienteid_persona(person_id)
            #Ida y vuelta a medilink a buscar el ID de la ultima cita creada para esta persona **IMPORTANTE** ULTIMA CITA CREADA
            actualizar_deal_cita_id(str(id_paciente), deal_id)
            #Una vez actualizado este campo en el trato vamos a buscar su valor
            id_cita = obtener_cita_deal_pipedrive(deal_id)
            #Valores de la cita indicada por el trato de pipedrive
            nombre_profesional, fecha, hora_inicio, hora_fin, duracion, estado_cita=detalles_cita_medilink(id_cita)
            #Llenar campos Deal
            pasar_medico_medilink_pipedrive(deal_id, id_cita, nombre_profesional)
            pasar_estado_medilink_pipedrive(deal_id, id_cita, estado_cita)
            estado_de_cita_pipedrive(deal_id, id_cita)
            #Hacemos las transformaciones a la hora_incio y duracion de la cita
            hora = adelantar_hora(hora_inicio, 3)
            duracion_pipedrive = convert_time(duracion)
            #Obtenemos user ID
            user_id=obtener_user_deal(deal_id)
            #Creamos Actividad
            crear_actividad(fecha, hora, duracion_pipedrive, deal_id, person_name, user_id)
            #Atrasamos la fecha en 1 dia
            fecha_atrasada = atrasar_fecha(fecha, 1)
            #Creamos la actividad de llamada para confirmar
            crear_actividad_confirmacion(fecha_atrasada, deal_id, person_name ,user_id)
            return ('Cita traida a Pipedrive', 200)
        except:
            pass
            return ('error, se trato y no se pudo pasar cita desde Medilink', 400)



    if stage_id == previous_stage_id and stage_id != 1:
        """ Esta funcionalidad actualiza el estado de la cita del de la cita cada vez que se edita el campo del Trato en Pipedrive. 
        Esta funcionalidad SOLO se activa como consecuencia de editar el campo del Trato en Pipedrive de manera inmediata. Esta parte de la integracion
        tiene efecto solo si es que el campo del trato ID Cita esta correctamente llenado como consecuencia de avanzar el trato desde la etapa 2 a la 3"""
        #Funciones interdependientes descritas abajo, utilizan el deal_id para cambiar cosas en la cita de medilink.
        id_cita= obtener_cita_deal_pipedrive(deal_id)

        try:
            try:
                #Extraemos el estado_cita
                string_estado_cita = extraer_estado_cita_pipedrive(deal_id)
                id_estado_cita_medilink = estado_cita_id(string_estado_cita)
                #Extraemos el modo de evaluacion, si esta vacio
                via_evaluacion = extraer_via_evaluacion_pipedrive(deal_id)
                #aplicamos la funcion que finalmente produce el update
                try: 
                    update_estado_cita_medilink(id_estado_cita_medilink, id_cita)
                    update_comentario_cita_medilink(via_evaluacion, id_cita)
                except:
                    update_comentario_cita_medilink(via_evaluacion, id_cita)
                return ('editado estado de cita en medilink', 200)
            except:
                via_evaluacion = extraer_via_evaluacion_pipedrive(deal_id)
                try:
                    update_comentario_cita_medilink(via_evaluacion, id_cita)
                except:
                    pass
                return ('editado comentario cita', 200)

        except:
            pass
            return ('accion no editable en medilink', 400)

    else:
        return ('no aplica')


##################################################    Funciones para etapa 1 =====>>> etapa 2    ####################################################################################

#Obtener datos persona
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
    sexo_id= person['data']['f18049fa9810fac7cba5bfb3c3deb6289a17a87a']
    sexo = obtener_sexo(sexo_id, client)

    return first_name, last_name, fecha_nacimiento, telephone, mail, cel, prevision, rut, ciudad, comuna, sexo

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

def validacion_paciente_agregado(person_id, mensaje):
    fields = client.persons.get_person_fields()
    data = fields['data']
    for field in data:
        if field['id']==9094:
            field_key = field['key']
    update={
        str(field_key): mensaje
    }
    response = client.persons.update_person(str(person_id), update)
    return ('validado')


def PatientID_RUT(rut):
    url= medilink_url + "pacientes"

    params = {
        'rut':{'eq':rut}
        }
    query_json = json.dumps(params)

    params={
        'q': query_json
    }
    headers = {
        'Authorization': 'Token ' + key_medilink
    }
    response = requests.get(url, headers=headers, params=params)
    response_json=response.json()
    for item in response_json['data']:
        patient_id = item['id'] 
        return str(patient_id)

### post patient ID as a Person Field
def update_person_pipedrive(person_id, patient_id):
    data = {
        '6c9143567d727245df03896004ab996922b581b3': str(patient_id)
    }
    response = client.persons.update_person(str(person_id), data)
    return ('exito')

def agregar_paciente(person_id, first_name, last_name, mail, rut, fecha_nacimiento, telephone, cel, sexo, ciudad, comuna):
    """ Agregamos paciente en medilink con los datos de la persona en Pipedrive """
    url_paciente= medilink_url + "pacientes"
    payload = {
            'nombre': first_name,
            'apellidos' : last_name,
            'email': mail,
            'rut': rut,
            "fecha_nacimiento" : fecha_nacimiento,
            'celular': telephone,
            'ciudad': ciudad,
            'comuna':comuna,
            'sexo':sexo
        }
    headers = {
        'Authorization': 'Token ' + key_medilink
    }
    response = requests.post(url_paciente, headers=headers, data = payload)
    response_json = response.json()
    mensaje = response_json['error']['message']

    if response.status_code == 201:
        """ Si la respuesta es 201 significa que se creo un nuevo paciente en medilink
        Obtenemos el identificador de este paciente en el sistema de medilink como la variable "paciente_id". """
        validacion_paciente_agregado(person_id, str(mensaje))
        json_response = response.json()
        paciente_id = json_response['data']['id']
        return str(paciente_id)
    elif response.status_code == 400 :
        """ Si obtenemos esta respuesta significa que el paciente ya existe en medilink, editamos los datos en el sistema
        para obtener el identificador del paciente """
        validacion_paciente_agregado(person_id, str(mensaje))
        paciente_id = PatientID_RUT(rut)
        json_respuesta = response.json()
        mensaje = json_respuesta['error']['message']
        return str(paciente_id)
    else:
        paciente_id = PatientID_RUT(rut)
        return str(paciente_id)



##################################################    Funciones para etapa 2 =====>>> etapa 3    ####################################################################################
    
#Extraer paciente ID de la persona
def extraer_pacienteid_persona(person_id):
    person = client.persons.get_person(str(person_id))
    id_paciente = person['data']['6c9143567d727245df03896004ab996922b581b3']
    return id_paciente

#Obtenemos la ultima cita de este paciente y alimentamos el campo del Deal que mapea el trato con la cita
def actualizar_deal_cita_id(id_paciente, deal_id):
    url = medilink_url + "pacientes/"+ str(id_paciente) + "/citas"
    headers = {
            'Authorization': 'Token ' + key_medilink
        }
    response = requests.get(url, headers=headers)
    response_json=response.json()
    #seleccionar ultima cita agregada
    citas = response_json['data']
    id_cita_reciente = citas[0]['id']
    #Postear el id de la cita mas recientemente creada
    data = {
        '9f86108fb2cc630f58c8be3ba54a3850f127d586': str(id_cita_reciente)
    }
    response = client.deals.update_deal(str(deal_id), data)
    return str(id_cita_reciente)

#Esta funcion obtiene el id de la cita en Medilink proveniente del campo del trato previamente llenado en Pipedrive
def obtener_cita_deal_pipedrive(deal_id):
    response = client.deals.get_deal_details(str(deal_id))
    id_cita_deal = response['data']['9f86108fb2cc630f58c8be3ba54a3850f127d586']
    return id_cita_deal

#Extraemos la informacion de la cita señalada en el campo ID Cita del trato en Pipedrive, este identificador es el que marcará la cita ascoiada al trato
#Cabe recordar que esta cita se rellena automaticamente al hacer el paso de la etapa 2 a la 3
def detalles_cita_medilink(id_cita):
    url = medilink_url + "citas/"+ f'{id_cita}' 
    headers = {
            'Authorization': 'Token ' + key_medilink
        }
    response = requests.get(url, headers=headers)
    response_json=response.json()
    #seleccionar ultima cita agregada
    cita = response_json['data']
    nombre_profesional = cita['nombre_profesional']
    fecha = cita['fecha']
    hora_inicio = cita['hora_inicio']
    hora_fin = cita['hora_fin']
    duracion = cita['duracion']
    estado_cita = cita['estado_cita']
    return nombre_profesional, fecha, hora_inicio, hora_fin, duracion, estado_cita

#Estas 2 funciones transforman le hora de inicio y la duracion respectivamente para crear la actividad en pipedrive
def adelantar_hora(string_hora, offset):
    strip = string_hora[:-3]
    hora = datetime.strptime(strip, "%H:%M")
    hora_adelantada = hora + timedelta(hours=int(offset))
    transformada = datetime.strftime (hora_adelantada, "%H:%M")
    return str(transformada)

#Convierte duracion en entero en strings para pipedrive
def convert_time(duracion):
    h, m = divmod(duracion, 60)
    time_trans = time(h, m)
    hora = time_trans.strftime("%H:%M")
    return hora

#Atrasa la fecha en "offset" dias
#Funcion que atrasa la fecha en n dias
def atrasar_fecha(date, dias_atraso):
    fecha = datetime.strptime(date, "%Y-%M-%d")
    fecha_atrasada = fecha - timedelta(days=dias_atraso)
    transformada = datetime.strftime(fecha_atrasada, "%Y-%M-%d")
    return transformada

#Obtener user ID
def obtener_user_deal(deal_id):
    response = client.deals.get_deal(str(deal_id))
    user_id = response['data']['user_id']['id']
    return user_id

#Esta funcion crea una actividad en Pipedrive que corresponde a la cita indicada en Medilink.
#create an activity
def crear_actividad(fecha, hora, duracion, deal_id, person_name, user_id):

    data = {
        'subject': 'EVALUACION ' + str(person_name),
        'type': 'meeting',
        'due_date': str(fecha),
        'due_time': str(hora),
        'duration': str(duracion),
        'user_id' : int(user_id),
        'deal_id' : int(deal_id)


    }
    response = client.activities.create_activity(data)
    return ('creado')

#Esta funcion crea una actividad el dia anterior a la cita del paciente para confirmar
def crear_actividad_confirmacion(fecha_atrasada, deal_id, person_name, user_id):
    data = {
        'subject': 'Confirmar cita ' + str(person_name),
        'type': 'Llamada para confirmar',
        'due_date': str(fecha_atrasada),
        'user_id' : int(user_id),
        'deal_id' : int(deal_id)

    }
    response = client.activities.create_activity(data)
    return ('creado') 

#Pasar el doctor de la cita a Pipedrive
def pasar_medico_medilink_pipedrive(deal_id, id_cita, nombre_profesional):
    #Pasar ese nombre al mismo deal en pipedrive en el campo Medico Evluador
    response = client.deals.get_deal_fields()
    data = response['data']
    for field in data:
        if field['key'] == 'ac17157b288339b2aa74ee205ab6897b6b0ab98d':
            opciones = field['options']
            for op in opciones:
                if op['label']==str(nombre_profesional):
                    id_doctor_medilink = (op['id'])
                else:
                    pass
        else:
            pass
    data = {
            'ac17157b288339b2aa74ee205ab6897b6b0ab98d': str(id_doctor_medilink)
        }
    response = client.deals.update_deal(str(deal_id), data)
    return ('pasado medico')

#Pasar estado de la cita a Pipedrive
def pasar_estado_medilink_pipedrive(deal_id, id_cita, estado_cita):
    #Pasar ese nombre al mismo deal en pipedrive en el campo Medico Evluador
    response = client.deals.get_deal_fields()
    data = response['data']
    for field in data:
        if field['key'] == '8a7fdbb5bfba1ad7d974548a957f49a7c669c68b':
            opciones = field['options']
            for op in opciones:
                if op['label']==str(estado_cita):
                    estado_cita_id = (op['id'])
                else:
                    pass
        else:
            pass
    data = {
            '8a7fdbb5bfba1ad7d974548a957f49a7c669c68b': str(estado_cita_id)
        }
    response = client.deals.update_deal(str(deal_id), data)
    return ('pasado estado')

def estado_de_cita_pipedrive(deal_id, id_cita):
    url = "https://api.medilink.healthatom.com/api/v1/citas/" + str(id_cita)
    headers = {
            'Authorization': 'Token ' + 'l6lzgzzhXaWEKzgddUIzyJLHFHcFxxCyNOHBuwV9.ISrSwKo4MwsqVrTOfAWQAsW6SY6W5DuGjbc8AhEF'
        }
    response = requests.get(url, headers=headers)
    response_json=response.json()
    estado_cita_string = response_json['data']['estado_cita']
    fields = client.deals.get_deal_fields()
    for field in fields['data']:
        if field['key']== '8a7fdbb5bfba1ad7d974548a957f49a7c669c68b':
            options=field['options']
            for option in options:
                if option['label']==str(estado_cita_string):
                    estado_de_cita_id=option['id']
                else:
                    pass
        else:
            pass
    data = {
        '8a7fdbb5bfba1ad7d974548a957f49a7c669c68b': str(estado_de_cita_id)
    }
    response = client.deals.update_deal(str(deal_id), data)
    return ('pasado estado')


############################################################## Update Estados de cita en Pipedrive #########################################################

#Extraer datos de la persona en Pipedrive
def extraer_datos_persona_pipedrive(person_id):
    """ Con el person_id obtenemos el rut asociado a esa persona en pipedrive """
    person = client.persons.get_person(str(person_id))
    rut=person['data']['5df3dc703fa5f7dae88df356d3542c794cb32295']
    return str(rut)

#Extraer el estado de cita del deal en Pipedrive
def extraer_estado_cita_pipedrive(deal_id):
    """ con el deal_id podemos saber el string del estado de cita una vez que este se edita y la funcion se activa, las siguientes opciones en pipedrive son:
    - No confirmado
    - Confirmado por teléfono
    - Confirmado por WhatsApp
    - Confirmado por e-mail
    - En sala de espera
    - Atendiéndose
    - Atendido
    - No asiste
    - Anulado
    Estas opciones deben existir de manera identica en medilink para que las actualizaciones sirvan """
    response = client.deals.get_deal_details(str(deal_id))
    estado_cita_deal_id= response['data']['8a7fdbb5bfba1ad7d974548a957f49a7c669c68b']
    #Hay que ir a buscar el valor string que corresponde a este indice
    fields = client.deals.get_deal_fields()
    for field in fields['data']:
        if field['key']=='8a7fdbb5bfba1ad7d974548a957f49a7c669c68b':
            list_options = field['options']
            for options in list_options:
                if options['id'] == int(estado_cita_deal_id):
                    estado_cita = options['label']
                else:
                    pass
        else:
            pass
    return str(estado_cita)


#Extraer el modo de evaluacion del deal en Pipedrive
""" con el deal_id podemos saber el string de la via de ebvaluacion:
- Presencial
- Zoom
- Fotos
Estas opciones serán enviadas a medilink como string """
def extraer_via_evaluacion_pipedrive(deal_id):
    response = client.deals.get_deal_details(str(deal_id))
    id_via_evaluacion= response['data']['d8442a1802dd22744636261f16066d0553459914']
    #Hay que ir a buscar el valor string que corresponde a este indice
    fields = client.deals.get_deal_fields()
    data = fields['data']
    for field in data:
        if field['key'] == 'd8442a1802dd22744636261f16066d0553459914':
            list_options = field['options']
            for options in list_options:
                try:
                    if options['id'] == int(id_via_evaluacion):
                        via_evaluacion = options['label']
                        return via_evaluacion
                except:
                    pass
                    return None

#Debemos obetener el id_cita asociada a la persona. El identificador de la cita es lo unico que nos permite editar el estado de esta en medilink cuando editamos en pipedrive.

#Obtener cita paciente
def obtener_cita_deal_pipedrive(deal_id):
    response = client.deals.get_deal_details(str(deal_id))
    id_cita_deal = response['data']['9f86108fb2cc630f58c8be3ba54a3850f127d586']
    return id_cita_deal


#Debemmos editar el estado de la cita, buscamos los estados de las citas y vemos que calzen con el estado en pipedrive obtenido de la funcion extraer_estado_cita_pipedrive
def estado_cita_id(string_estado):
    """ Input es el string que se consigue del estado de la cita en pipedrive, este se ocupa para
    mandarselo como payload al PUT de medilink """
    url = medilink_url + "citas/estados"
    headers = {
            'Authorization': 'Token ' + key_medilink
        }
    response = requests.get(url, headers=headers)
    response_json=response.json()
    estados=response_json['data']
    for estado in estados:
        if estado['nombre']==str(string_estado):
            id_estado_cita = estado['id']
        else:
            pass
    return str(id_estado_cita)

def update_estado_cita_medilink(id_estado_cita, id_cita):
    """ Esta funcion toma el id_cita y el id_estado_cita obtenida previamente y actualiza el estado en medilink """
    url = medilink_url + "citas/"+ str(id_cita)
    payload = {
        "id_estado": int(id_estado_cita)
    }

    headers = {
            'Authorization': 'Token ' + key_medilink
        }
    response = requests.put(url, headers=headers, data=payload)
    if response.status_code == 200:
        print('cambio exitoso')
    if response.status_code == 400:
        print('estado es el mismo que el original o hubo un error')
    return ('Update hecho')

def update_comentario_cita_medilink(via_evaluacion, id_cita):
    """ Esta funcion toma el id_cita y el id_estado_cita obtenida previamente y actualiza el estado en medilink """
    url = medilink_url + "citas/"+ str(id_cita)
    payload = {
        "comentario": str(via_evaluacion)
    }

    headers = {
            'Authorization': 'Token ' + key_medilink
        }
    response = requests.put(url, headers=headers, data=payload)
    if response.status_code == 200:
        print('cambio comentario exitoso')
    if response.status_code == 400:
        print('comentario es el mismo que el original o hubo un error')
    return ('Update hecho')

