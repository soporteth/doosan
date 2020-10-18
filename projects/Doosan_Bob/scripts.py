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