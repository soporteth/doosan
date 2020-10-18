from pipedrive.client import Client
import requests
import pandas as pd
import json
from datetime import datetime, time, timedelta, date
from dateutil import relativedelta
import numpy as np

key_pipedrive = 'd8afbc8ea9d8baac3780fb5c0fdb14f3457dce74'
client = Client(domain='https://doosanbobcatchile.pipedrive.com/')
client.set_api_token (key_pipedrive)
filter_id = 261
#Bulk Extract from Pipedrive API
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

Scoring_pronostico={
    'f71ff7a4c979ad4631fbb2127a9a5a0083b02ecb': 'SCORING pronóstico'
}
campos_scoring={
    '50fcc47f936a5b0f3b1349838650d15bbfb5601c':'[P] Fecha de cierre indicada por CLIENTE',
    '499b041f3cbae41bc507efbddd19ba13f91390da': '[P] Tipo de negocio',
    'ba9b8d61f576340fdc732a9f7b440483955a7342': '[P] Preferencia de marca a cerrar',
    '990ed7f17dedfc134fdc3342ef1fa676aba9cefc': '[P] ¿Existe mandante final?',
    '6258c65f1784bd0c06beb940886eebff987fb5a7': '[P] Disponibilidad del equipo al cierre',
    'dc604dd4b9f281792c0edc8717539f689532f4f6': '[P] Necesidad de financiamiento',
    'expected_close_date': 'Fecha prevista de cierre'
}

persona_rol={
    'c0305a29aa539979002af528ebaadf5b55b12bb3': '[P] Rol'
}

def get_person_data(df):
    id_persona = []
    lista_rol =[]
    for person in df['person_id']:
        try:
            value = person.get('value')
            id_persona.append(int(value))
            try: 
                response = client.persons.get_person(str(value))
                rol_id = response['data']['c0305a29aa539979002af528ebaadf5b55b12bb3']
                lista_rol.append(rol_id)    
            except:
                lista_rol.append(None)
        except:
            id_persona.append(None)
            lista_rol.append(None)
    df['identificador_persona']=id_persona
    df['rol']=lista_rol
    return df


#Reglas para Dataframe

def fecha_provista_cierre(date):
    try:
        fecha = datetime.strptime(date, "%Y-%m-%d")
        today = datetime.today()
        month_today = today.month
        month_fecha = fecha.month
        if month_today==month_fecha:
            score=5
            return score
        elif month_today < month_fecha:
            score = 0
            return score
        else:
            score = 5   
            return score
    
    except:
        score=None
        return score

def fecha_indicada_cierre(date):
    try:
        fecha = datetime.strptime(date, "%Y-%m-%d")
        today = datetime.today()
        month_today = today.month
        month_fecha = fecha.month
        if month_today==month_fecha:
            score=20
        elif month_today < month_fecha:
            score = 5
        else:
            score = 20
        return score
    except:
        score = None
        return score

def rol_scoring(rol):
    try:
        if rol == str(866):
            score = 10
        elif rol == str(867):
            score=5
        elif rol == str(868):
            score=0
        else:
            score = None
    except:
        score = None
    return score

def scoring_tipo_negocio(tipo):
    try:
        if tipo == str(1043):
            score = 10
        elif tipo == str(1044):
            score = 5
        elif tipo == str(1045):
            score = 2
        else:
            score = None
        return score
    except:
        score = None
        return score

def scoring_preferencia_marca(marca):
    try:
        if marca == str(1046):
            score = 20
        elif marca == str(1047):
            score = 10
        else:
            score = None
        return score
    except:
        score = None
        return score

def scoring_mandante_final(existe):
    try:
        if existe == str(1048):
            score = 15
        elif existe == str(1049):
            score=10
        elif existe == str(1050):
            score = 0
        else:
            score = None
        return score
    except:
        score = None
        return score
def disponibilidad_equipo(disp):
    try:
        if disp == str(1051):
            score = 10
        elif disp == str(1052):
            score =0
        else:
            score = None
        return score
    except:
        score=None
        return score

def necesidad_financiamiento(need):
    try:
        if need == str(1053):
            score = 10
        elif need == str(1054):
            score = 10
        elif need == str(1055):
            score = 4
        elif need == str(1056):
            score = 2
        elif need== str(1066):
            score = 0
        else:
            score=None
        return score
    except:
        score=None
        return score


lista_campos = [
    'id',
    'rol',
    'expected_close_date',
    'fecha_prevista_scoring',
    'fecha_indicada_scoring',
    'Rol_scoring',
    'Tipo_negocio_scoring',
    'Preferencia_marca_scoring',
    'Existe_mandante_scoring',
    'Disponibilidad_equipo_scoring',
    'Necesidad_financiamiento_scoring',
    'Score'
]

def scoring(df):
    df['fecha_prevista_scoring'] = df['expected_close_date'].apply(fecha_provista_cierre)
    df['fecha_indicada_scoring'] = df['50fcc47f936a5b0f3b1349838650d15bbfb5601c'].apply(fecha_indicada_cierre)
    df['Rol_scoring']= df['rol'].apply(rol_scoring)
    df['Tipo_negocio_scoring']= df['499b041f3cbae41bc507efbddd19ba13f91390da'].apply(scoring_tipo_negocio)
    df['Preferencia_marca_scoring']= df['ba9b8d61f576340fdc732a9f7b440483955a7342'].apply(scoring_preferencia_marca)
    df['Existe_mandante_scoring']= df['990ed7f17dedfc134fdc3342ef1fa676aba9cefc'].apply(scoring_mandante_final)
    df['Disponibilidad_equipo_scoring']= df['6258c65f1784bd0c06beb940886eebff987fb5a7'].apply(disponibilidad_equipo)
    df['Necesidad_financiamiento_scoring']= df['dc604dd4b9f281792c0edc8717539f689532f4f6'].apply(necesidad_financiamiento)
    df['Score']=df.iloc[:, -9:-1].sum(axis=1)
    new_df = df[lista_campos]
    new_df = new_df.fillna(value=np.nan)
    return new_df

def deals_score(df):
    deals = []
    rango = df.id.count()
    for row in range(rango):
        if df.iloc[row, :-1].isnull().values.any() == False:
            deal_id = df.iloc[row, 0]
            score = df.iloc[row, -1]
            par = [deal_id, score]
            deals.append(par)
        else:
            pass
    return deals

def llenar_score(lista):
    for deal in lista:
        data = {
            'f71ff7a4c979ad4631fbb2127a9a5a0083b02ecb': int(deal[1])
        }
        response = client.deals.update_deal(str(deal[0]), data)
    return 'score calculado'

def vaciar_score(lista):
    for deal in lista:
        data = {
            'f71ff7a4c979ad4631fbb2127a9a5a0083b02ecb': None
        }
        response = client.deals.update_deal(str(deal[0]), data)
    return 'score calculado'

def vaciar_score_todo(df):
    lista = list(df['id'])
    for deal in lista:
        data = {
            'f71ff7a4c979ad4631fbb2127a9a5a0083b02ecb': None
        }
        response = client.deals.update_deal(str(deal), data)
    return 'score calculado'

def score_engine(request):
    deals = deals_bulk_extract(key_pipedrive, filter_id)
    dataframe = get_person_data(deals)
    new_df = scoring(dataframe)
    deals = deals_score(new_df)
    vaciar_score(deals)
    llenar_score(deals)
    return 'score calculado'





