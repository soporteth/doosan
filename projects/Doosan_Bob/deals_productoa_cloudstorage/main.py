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
        lista_deals_id = list(df['id'])
    return lista_deals_id


campos={
    'deal_id',
    'product_id',
    'item_price',
    'discount_percentage',
    'sum',
    'sum_formatted',
    'name',
    'quantity'
}

def productos_en_deal(key_pipedrive, deal_id):
    start_page =0
    url = 'https://api.pipedrive.com/v1/deals/' +f'{deal_id}' + '/products?start=0&api_token=' + key_pipedrive
    headers = {
            'Accept': 'application/json'
        }
    response = requests.get(url, headers=headers)
    json_deals = response.json()
    df = pd.DataFrame.from_dict(json_deals['data'])
    return json_deals['data']


def union(lista_deals):
    lista_final = []
    for deal in lista_deals:
        try:
            lista = productos_en_deal(key_pipedrive, deal)
            for product in lista:
                try:
                    lista_final.append(product)
                except:
                    pass
        except:
            pass
    return lista_final


def products_deal():
    lista_deals = deals_bulk_extract(key_pipedrive, filter_id)
    lista_final = union(lista_deals)
    print(lista_deals)
    print(len(lista_deals))
    print(len(lista_final))
    df = pd.DataFrame.from_dict(lista_final)
    df = df[campos]
    print(df)
    #Define Storage Bucket
    #storage.Bucket('doosan-bobcat-csv').item('products_in_deal.csv').write_to(df.to_csv(index = False),'text/csv')
    return 'Ok'

products_deal()
