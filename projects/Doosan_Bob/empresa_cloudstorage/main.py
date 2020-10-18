from pipedrive.client import Client
import requests
import pandas as pd
import json

import datalab.storage as storage


#Pipedrive cred
key_pipedrive = 'd8afbc8ea9d8baac3780fb5c0fdb14f3457dce74'
client = Client(domain='https://doosanbobcatchile.pipedrive.com/')
client.set_api_token (key_pipedrive)

def obtener_campo_org():
    url = 'https://api.pipedrive.com/v1/organizationFields?api_token=' + key_pipedrive
    headers = {
            'Accept': 'application/json'
        }
    response = requests.get(url, headers=headers)
    json_fields = response.json()
    return json_fields


def organisation_bulk_extract(key_pipedrive):
    start_page =0
    url = 'https://api.pipedrive.com/v1/organizations?start=' +f'{start_page}' +'&api_token=' + key_pipedrive
    headers = {
            'Accept': 'application/json'
        }
    response = requests.get(url, headers=headers)
    json_org = response.json()
    df = pd.DataFrame.from_dict(json_org['data'])
    while json_org['data'] is not None:
        start_page = start_page + 100
        url_temp = 'https://api.pipedrive.com/v1/organizations?start=' +f'{start_page}' +'&api_token=' + key_pipedrive
        headers = {
            'Accept': 'application/json'
        }
        response= requests.get(url_temp, headers=headers)
        json_org= response.json()
        df_new= pd.DataFrame.from_dict(json_org['data'])
        df = df.append(df_new, ignore_index=True)
    return df

def replace_characters2(name):
    characters = ['/', '%']
    try:
        for char in characters:
            new_str = name.replace(char, '')
    except:
        new_str = name
    return new_str

def id_int(nro):
    try:
        ID = int(nro)
    except:
        ID = None
    return ID

def owner_id(nro):
    try:
        owner = int(nro['id'])
    except:
        owner = None
    return owner

def comuna(nro):
    for field in fields['data']:
        if field['key']=='f2bcb1426806560f1f73c7682cac6a50eb9af740':
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

def ciudad(nro):
    for field in fields['data']:
        if field['key']=='c041c19af6179cb72d107de4bdf856eabf86fef3':
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

def sucursal(nro):
    for field in fields['data']:
        if field['key']=='47df981fa98274c948733b511d6482f2185bd610':
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

def cliente_estrategico(nro):
    for field in fields['data']:
        if field['key']=='0d432724053f82c9ba92108ba31a4b0819cf4d53':
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

def volumen_venta(nro):
    for field in fields['data']:
        if field['key']=='88be3ae5fa64553deccb5ccbfc4a7269602b8226':
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

def cantidad_proyectos(nro):
    for field in fields['data']:
        if field['key']=='cd6c7a87c1a755b9770f207236c8c92843b152fc':
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

def oportunidad_doosan(nro):
    for field in fields['data']:
        if field['key']=='630d9f54ed79364dcabcca70929f8dd23bd06dd7':
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

def venta_alternativa(nro):
    for field in fields['data']:
        if field['key']=='bf4c18663357053f7626984401f243826ebf74d5':
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

def replace_characters(name):
    try:
        str_new = name.encode('utf-8')
    except:
        str_new = name  
    return str_new



lista_campos = [
    'id',
    'owner_id',
    'nombre',
    'open_deals_count',
    'closed_deals_count',
    'won_deals_count',
    'lost_deals_count',
    'update_time',
    'next_activity_date',
    'last_activity_date',
    'comuna',
    'ciudad',
    'sucursal',
    'cliente_estrategico',
    'volumen_venta',
    'cantidad_proyectos',
    'oportunidad_doosan',
    'venta_alternativa'

    ]

def transform_df(df):
    df['id_org'] = df['id'].apply(id_int)
    df['owner_id'] = df['owner_id'].apply(owner_id)
    df['nombre'] = df['name'].apply(replace_characters)
    df['comuna'] = df['f2bcb1426806560f1f73c7682cac6a50eb9af740'].apply(comuna)
    df['ciudad'] = df['c041c19af6179cb72d107de4bdf856eabf86fef3'].apply(ciudad)
    df['sucursal'] = df['47df981fa98274c948733b511d6482f2185bd610'].apply(sucursal)
    df['cliente_estrategico'] = df['0d432724053f82c9ba92108ba31a4b0819cf4d53'].apply(cliente_estrategico)
    df['volumen_venta'] = df['88be3ae5fa64553deccb5ccbfc4a7269602b8226'].apply(volumen_venta)
    df['cantidad_proyectos'] = df['cd6c7a87c1a755b9770f207236c8c92843b152fc'].apply(cantidad_proyectos)
    df['oportunidad_doosan'] = df['630d9f54ed79364dcabcca70929f8dd23bd06dd7'].apply(oportunidad_doosan)
    df['venta_alternativa'] = df['bf4c18663357053f7626984401f243826ebf74d5'].apply(venta_alternativa)
    df['update_time'] = pd.to_datetime(df['update_time'], errors='coerce')
    df['next_activity_date'] = pd.to_datetime(df['next_activity_date'], errors='coerce')
    df['last_activity_date'] = pd.to_datetime(df['last_activity_date'], errors='coerce')
    new_df = df[lista_campos]
    return new_df

fields = obtener_campo_org()


def create_organisation_table(request):
    organisations = organisation_bulk_extract(key_pipedrive)
    df_org = transform_df(organisations)
    storage.Bucket('doosan-bobcat-csv').item('organisations.csv').write_to(df_org.to_csv(index = False),'text/csv')
    return 'Organisations creados'

