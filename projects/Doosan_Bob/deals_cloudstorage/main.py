from pipedrive.client import Client
import requests
import pandas as pd
import json
import numpy as np

import datalab.storage as storage


#Pipedrive cred
key_pipedrive = 'd8afbc8ea9d8baac3780fb5c0fdb14f3457dce74'
client = Client(domain='https://doosanbobcatchile.pipedrive.com/')
client.set_api_token (key_pipedrive)
filter_id = 261
fields = client.deals.get_deal_fields()



def deals_bulk_extract(key_pipedrive, filter_id):
    start_page =0
    url = 'https://api.pipedrive.com/v1/deals?filter_id='+f'{filter_id}' + '&status=all_not_deleted&start=' +f'{start_page}' +'&api_token=' + key_pipedrive
    payload = {
        'filter_id':str(filter_id)
    }
    headers = {
            'Accept': 'application/json'
        }
    response = requests.get(url, headers=headers, data=payload)
    json_deals = response.json()
    df = pd.DataFrame.from_dict(json_deals['data'])
    while json_deals['data'] is not None:
        start_page = start_page + 100
        url_temp = 'https://api.pipedrive.com/v1/deals?filter_id='+f'{filter_id}' + '&status=all_not_deleted&start=' +f'{start_page}' +'&api_token=' + key_pipedrive
        headers = {
            'Accept': 'application/json'
        }
        response= requests.get(url_temp, headers=headers)
        json_deals= response.json()
        df_new= pd.DataFrame.from_dict(json_deals['data'])
        df = df.append(df_new, ignore_index=True)
    return df

lista_campos = [
    'id',
    'person_id',
    'org_id',
    'creator_user_id',
    'title',
    'f71ff7a4c979ad4631fbb2127a9a5a0083b02ecb',
    'label',
    'update_time',
    'last_activity_date',
    'stage_change_time',
    'owner_name',
    'expected_close_date',
    'status',
    '50fcc47f936a5b0f3b1349838650d15bbfb5601c',
    '499b041f3cbae41bc507efbddd19ba13f91390da',
    'ba9b8d61f576340fdc732a9f7b440483955a7342',
    '990ed7f17dedfc134fdc3342ef1fa676aba9cefc',
    '6258c65f1784bd0c06beb940886eebff987fb5a7',
    'dc604dd4b9f281792c0edc8717539f689532f4f6',
    'value',
    'weighted_value',
    'currency',
    'products_count',
    'stage_id',
    'add_time',
    '2330257029ddcf8943a1bb44522d854540269b05',
    '0427f7ce265b3216bbbdb9697915675cd5624869',
    '53755ceee28cc036ce9b8ae7748a5a619ad91c57',
    '65b7eac9d4ca014c21d9cd3b5b1cc08ea060efcf',
    '36388b1a867b791d6dca124668869d884eb94267',
    '5cd88c1d4449ac4de77ceaec211f9cdbdedefb26'
    ]
def id_int(nro):
    try:
        ID = int(nro)
    except:
        ID = None
    return ID

def person_id(nro):
    try:
        person_id = int(nro['value'])
    except:
        person_id = None
    return person_id

def organisation_id(nro):
    try:
        org_id = nro['value']
    except:
        org_id = None
    return org_id

def creator_name(nro):
    try:
        name = nro['name']
    except:
        name = None
    return name

def creator_mail(nro):
    try:
        mail = nro['email']
    except:
        mail = None
    return mail

def fecha_cierre_por_cliente(nro):
    for field in fields['data']:
        if field['key']=='50fcc47f936a5b0f3b1349838650d15bbfb5601c':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def tipo_negocio(nro):
    for field in fields['data']:
        if field['key']=='499b041f3cbae41bc507efbddd19ba13f91390da':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def preferencia_marca(nro):
    for field in fields['data']:
        if field['key']=='ba9b8d61f576340fdc732a9f7b440483955a7342':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def existe_mandante(nro):
    for field in fields['data']:
        if field['key']=='990ed7f17dedfc134fdc3342ef1fa676aba9cefc':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def disponibilidad_equipo(nro):
    for field in fields['data']:
        if field['key']=='6258c65f1784bd0c06beb940886eebff987fb5a7':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def necesidad_financiamiento(nro):
    for field in fields['data']:
        if field['key']=='dc604dd4b9f281792c0edc8717539f689532f4f6':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def stages(nro):
    if nro == '1':
        label = 'Prospecto ingresado'
    elif nro == '2':
        label = 'Contacto establecido'
    elif nro == '3':
        label = 'Necesidades definidas'
    elif nro == '4':
        label = 'Propuesta enviada'
    elif nro == '5':
        label = 'En negociación'
    else:
        label = None
    return label

def origen_lead(nro):
    for field in fields['data']:
        if field['key']=='2330257029ddcf8943a1bb44522d854540269b05':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def marca_cotizar(nro):
    for field in fields['data']:
        if field['key']=='0427f7ce265b3216bbbdb9697915675cd5624869':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def que_requiere(nro):
    for field in fields['data']:
        if field['key']=='53755ceee28cc036ce9b8ae7748a5a619ad91c57':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def destino_final(nro):
    for field in fields['data']:
        if field['key']=='65b7eac9d4ca014c21d9cd3b5b1cc08ea060efcf':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def forma_pago(nro):
    for field in fields['data']:
        if field['key']=='36388b1a867b791d6dca124668869d884eb94267':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def rent_to_sell(nro):
    for field in fields['data']:
        if field['key']=='5cd88c1d4449ac4de77ceaec211f9cdbdedefb26':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def etiqueta(nro):
    for field in fields['data']:
        if field['key']=='label':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                label = option['label']
        except:
            label = None
    return label

def mail_vendedor(vendedor):
    if vendedor == '[V] Enrique Muñoz':
        mail = 'viajandoentrechile@gmail.com'
    elif vendedor == '[V] Pablo Leiva':
        mail = 'pablo.javier.leiva@gmail.com'
    elif vendedor == '[V] Hernán Varela':
        mail = 'hernanvarelatello@gmail.com'
    elif vendedor == '[V] Teodoro Poblete':
        mail == 'teodoropoblete@gmail.com'
    elif vendedor == '[V] Mario Márquez':
        mail = 'mario.welcome2018@gmail.com'
    elif vendedor == '[V] Jorge Wittwer':
        mail = 'wittwerjorge@gmail.com'
    elif vendedor == '[V] Santiago Avendaño':
        mail = 'saventasyservicios@gmail.com'
    elif vendedor == '[V] Juan Millan':
        mail = 'jmillanlatorre@gmail.com'
    else:
        mail = 'sin mail'
    return mail

lista_definitiva = [
    'id',
    'id_person',
    'organisation_id',
    'creator',
    'creator_mail',
    'title',
    'Scoring_pronostico',
    'etiqueta',
    'fecha_update',
    'fecha_ultima_act',
    'fecha_cambio_etapa',
    'owner_name',
    'fecha_estimada_cierre',
    'status',
    'fecha_cierre_indicada',
    'tipo_negocio',
    'preferencia_marca',
    'existe_mandante',
    'disponibilidad_equipo_cierre',
    'necesidad_financiamiento',
    'value',
    'weighted_value',
    'currency',
    'products_count',
    'stage',
    'fecha_creacion',
    'origen_lead',
    'marca_cotizar',
    'que_requiere',
    'destino_final',
    'forma_de_pago',
    'opcion_rent_to_sell',
    'mail_vendedor'
    ]


def transform_df(df):
    df['id_person'] = df['person_id'].apply(person_id)
    df['organisation_id'] = df['org_id'].apply(organisation_id)
    df['creator'] = df['creator_user_id'].apply(creator_name)
    df['creator_mail'] = df['creator_user_id'].apply(creator_mail)
    df['Scoring_pronostico'] = df['f71ff7a4c979ad4631fbb2127a9a5a0083b02ecb']
    df['fecha_cierre_indicada_cliente'] = df['50fcc47f936a5b0f3b1349838650d15bbfb5601c']
    df['tipo_negocio'] = df['499b041f3cbae41bc507efbddd19ba13f91390da'].apply(tipo_negocio)
    df['preferencia_marca'] = df['ba9b8d61f576340fdc732a9f7b440483955a7342'].apply(preferencia_marca)
    df['existe_mandante'] = df['990ed7f17dedfc134fdc3342ef1fa676aba9cefc'].apply(existe_mandante)
    df['disponibilidad_equipo_cierre'] = df['6258c65f1784bd0c06beb940886eebff987fb5a7'].apply(disponibilidad_equipo)
    df['necesidad_financiamiento'] = df['dc604dd4b9f281792c0edc8717539f689532f4f6'].apply(necesidad_financiamiento)
    df['stage'] = df['stage_id'].apply(stages)
    df['origen_lead'] = df['2330257029ddcf8943a1bb44522d854540269b05'].apply(origen_lead)
    df['marca_cotizar'] = df['0427f7ce265b3216bbbdb9697915675cd5624869'].apply(marca_cotizar)
    df['que_requiere'] = df['53755ceee28cc036ce9b8ae7748a5a619ad91c57'].apply(que_requiere)
    df['destino_final'] = df['65b7eac9d4ca014c21d9cd3b5b1cc08ea060efcf'].apply(destino_final)
    df['forma_de_pago'] = df['36388b1a867b791d6dca124668869d884eb94267'].apply(forma_pago)
    df['opcion_rent_to_sell'] = df['5cd88c1d4449ac4de77ceaec211f9cdbdedefb26'].apply(rent_to_sell)
    df['fecha_update'] = pd.to_datetime(df['update_time'], errors='coerce')
    df['fecha_ultima_act'] = pd.to_datetime(df['last_activity_date'], errors='coerce')
    df['fecha_cambio_etapa'] = pd.to_datetime(df['stage_change_time'], errors='coerce')
    df['fecha_estimada_cierre'] = pd.to_datetime(df['expected_close_date'], errors='coerce')
    df['fecha_cierre_indicada'] = pd.to_datetime(df['fecha_cierre_indicada_cliente'], errors='coerce')
    df['fecha_creacion'] = pd.to_datetime(df['add_time'], errors='coerce')
    df['etiqueta'] = df['label'].apply(etiqueta)
    df['id_person'] = df['id_person'].fillna(0).astype(np.int64)
    df['organisation_id'] = df['organisation_id'].fillna(0).astype(np.int64)
    df['mail_vendedor'] = df['owner_name'].apply(mail_vendedor)
    new_df = df[lista_definitiva]
    return new_df


def create_deals_table():
    deals = deals_bulk_extract(key_pipedrive, filter_id)
    df_deals = deals[lista_campos]
    df_transformado = transform_df(df_deals)
    df_transformado_final= df_transformado.fillna('')
    print(df_transformado_final['mail_vendedor'])
    #storage.Bucket('doosan-bobcat-csv').item('deals.csv').write_to(df_transformado_final.to_csv(index = False),'text/csv')
    return 'Deals creados'

create_deals_table()