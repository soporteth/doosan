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

def product_deals(product_id):
    start_page =0
    url = 'https://api.pipedrive.com/v1/products/' +f'{product_id}' +'/deals?start='+f'{start_page}' + '&status=all_not_deleted&api_token=' + key_pipedrive
    headers = {
            'Accept': 'application/json'
        }
    response = requests.get(url, headers=headers)
    json_deals = response.json()
    df = pd.DataFrame.from_dict(json_deals['data'])
    while json_deals['data'] is not None:
        start_page = start_page + 100
        url_temp = 'https://api.pipedrive.com/v1/products/' +f'{product_id}' +'/deals?start='+f'{start_page}' + '&status=all_not_deleted&api_token=' + key_pipedrive
        headers = {
            'Accept': 'application/json'
        }
        response= requests.get(url_temp, headers=headers)
        json_deals= response.json()
        df_new= pd.DataFrame.from_dict(json_deals['data'])
        df = df.append(df_new, ignore_index=True)
    if df.empty == True:
        return None
    else:
        return list(zip(df.id, df.products_count))



def conector(lista_products):
    conector_final = []
    for product in lista_products:
        lista_deals = product_deals(product)
        try:
            for deal in lista_deals:
                producto_id = str(product)
                deal_id = str(deal[0])
                product_count = str(deal[1])
                conector_final.append([producto_id, deal_id, product_count])
        except:
            pass
    return conector_final

def create_conector(request):
    products = products_bulk_extract(key_pipedrive)
    lista_products = list(products['id'])
    array = conector(lista_products)
    df = pd.DataFrame(array)
    df.columns = ["Product_id", "Deal_id", "Product_count"]
    #Define Storage Bucket
    storage.Bucket('doosan-bobcat-csv').item('conector.csv').write_to(df.to_csv(index = False),'text/csv')
    return 'Deals creados'


