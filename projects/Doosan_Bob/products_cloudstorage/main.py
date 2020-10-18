from pipedrive.client import Client
import requests
import pandas as pd
import json

import datalab.storage as storage


#Pipedrive cred
key_pipedrive = 'd8afbc8ea9d8baac3780fb5c0fdb14f3457dce74'
client = Client(domain='https://doosanbobcatchile.pipedrive.com/')
client.set_api_token (key_pipedrive)
filter_id = 261
fields = client.products.get_product_fields()



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

def products_bulk_extract(key_pipedrive):
    start_page =0
    url = 'https://api.pipedrive.com/v1/products?status=all_not_deleted&start=' +f'{start_page}' +'&api_token=' + key_pipedrive

    headers = {
            'Accept': 'application/json'
        }
    response = requests.get(url, headers=headers)
    json_deals = response.json()
    df = pd.DataFrame.from_dict(json_deals['data'])
    while json_deals['data'] is not None:
        start_page = start_page + 100
        url_temp = 'https://api.pipedrive.com/v1/products?status=all_not_deleted&start=' +f'{start_page}' +'&api_token=' + key_pipedrive
        headers = {
            'Accept': 'application/json'
        }
        response= requests.get(url_temp, headers=headers)
        json_deals= response.json()
        df_new= pd.DataFrame.from_dict(json_deals['data'])
        df = df.append(df_new, ignore_index=True)
    return df



def person_bulk_extract(key_pipedrive, filter_id):
    start_page =0
    url = 'https://api.pipedrive.com/v1/persons?' + '&start=' +f'{start_page}' +'&api_token=' + key_pipedrive
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
        url_temp = 'https://api.pipedrive.com/v1/persons?' + '&start=' +f'{start_page}' +'&api_token=' + key_pipedrive
        headers = {
            'Accept': 'application/json'
        }
        response= requests.get(url_temp, headers=headers)
        json_deals= response.json()
        df_new= pd.DataFrame.from_dict(json_deals['data'])
        df = df.append(df_new, ignore_index=True)
    return df

def producto_marca(nro):
    for field in fields['data']:
        if field['key']=='c3617ac02a660cbfbed35cd09fae9bf722958328':
            list_options = field['options']
        else:
            pass
    for option in list_options:
        try:
            if option['id'] == int(nro):
                marca = option['label']
        except:
            marca = None
    return marca

def producto_subcategoria(nro):
    for field in fields['data']:
        if field['key']=='6b093ea1108c1ade4742b47eaacad41761484161':
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

def producto_subcategoria_abr(nro):
    for field in fields['data']:
        if field['key']=='8cd24dda7f8f95dda6cadc16c54feae4261c1c1e':
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

def producto_publicar_venta(nro):
    for field in fields['data']:
        if field['key']=='240a23ccc546089a268c9a161cf021650013d1b2':
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

def producto_visibilidad(nro):
    for field in fields['data']:
        if field['key']=='visible_to':
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


def product_owner(nro):
    name = nro['name']
    return name

def active_flag(nro):
    if nro == True:
        label = 'Si'
    elif nro == False:
        label = 'No'
    else:
        label = None
    return label

def extract_price(nro):
    for obj in nro:
        try:
            price = obj['price']
        except:
            price = None
    return price

lista_campos = [
    'id',
    'name',
    'marca',
    'modelo',
    'price',
    'subcategoria',
    'subcategoria_abr',
    'publicar_venta',
    'owner',
    'activo',
    'visible_para'
    ]

def transform_df(df):
    df['marca'] = df['c3617ac02a660cbfbed35cd09fae9bf722958328'].apply(producto_marca)
    df['subcategoria'] = df['6b093ea1108c1ade4742b47eaacad41761484161'].apply(producto_subcategoria)
    df['subcategoria_abr'] = df['8cd24dda7f8f95dda6cadc16c54feae4261c1c1e'].apply(producto_subcategoria_abr)
    df['publicar_venta'] = df['240a23ccc546089a268c9a161cf021650013d1b2'].apply(producto_publicar_venta)
    df['owner'] = df['owner_id'].apply(product_owner)
    df['activo'] = df['active_flag'].apply(active_flag)
    df['visible_para'] = df['visible_to'].apply(producto_visibilidad)
    df['price'] = df['prices'].apply(extract_price)
    df['modelo'] = df['bd7a7bc0229af5cb48a9c601d78a05ed4095cfec']
    new_df = df[lista_campos]
    return new_df



def create_product_table(request):
    products_df = products_bulk_extract(key_pipedrive) 
    df_p = products_df[['id', 'name', 'c3617ac02a660cbfbed35cd09fae9bf722958328', '6b093ea1108c1ade4742b47eaacad41761484161', '8cd24dda7f8f95dda6cadc16c54feae4261c1c1e', '240a23ccc546089a268c9a161cf021650013d1b2','bd7a7bc0229af5cb48a9c601d78a05ed4095cfec', 'owner_id','prices','active_flag', 'visible_to']]
    dataframe = transform_df(df_p)
    storage.Bucket('doosan-bobcat-csv').item('productos.csv').write_to(dataframe.to_csv(index = False),'text/csv')
    return 'Products creados'
