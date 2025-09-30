import requests
import json
import pandas as pd 
from pandas import json_normalize

from src.conectores.conecta_agendor import CredencialApi

import datetime
from datetime import timedelta, timezone

import boto3

def create_lead_agendor(lead, tag_type, product_type):

    # credencial no aws
    cred = CredencialApi()
    credentials = cred.get_agendor_token()
    
    token = credentials['token']

    ##
    ## BUSCAR USUARIO ###
    ##

    # Buscar usuario no agendor nos casos abaixo:

    #if tag_type == 'DIR' and product_type == 'Simples':

    # DIR=INTERNO | SAFIRA SIMPLES
    if tag_type == 'DIR':
        print('verificando user no simples interno...')
        user_id = get_user_id(lead['custom_fields']['[MKT] Responsável'], token)

    # EXT | FEI= EXTERNO FEIRA SAFRIA LIVER
    elif product_type == 'Livre' and tag_type in ['EXT', 'FEI']:
        print('verificando user no livre...')
        user_id = get_user_id(lead['custom_fields']['[Livre] Responsável'], token)

    # SAFIRA PADRÃO | SIMPLES
    else:
        print('verificando user no simples padrão...')
        user_id = 669907

    # NÃO HA PARA SAFIRA SOLAR | OUTRO CRM

    ##
    ## INICIO DO PROCESSO ###
    ##

    # Criar company no agendor usando token gerado
    print('Creating company...')

    company_id = create_company_agendor(lead, user_id, product_type, token)

    print(f'company id: {company_id}')

    # Criar pessoa no agendor usando token gerado
    print('Creating person...')    

    person_id = create_person_company(lead, user_id, company_id, token)

    print(f'person id: {person_id}')

    # Checar dados enviados foram criados no agendor usando token gerado
    check_deal_creation = check_if_can_create_lead(company_id, token)

    print(f"Can create: {check_deal_creation}")


    ##
    ## VALIDAÇÃO DOS DADOS - GERADOS COM SUCESSO NO AGENDOR ###
    ##

    # caso dados foram criados recber True, cria negociação associada ao user, company do agendor

    if check_deal_creation == True:
        # Create deal
        print('Creating deal...')
        
        create_deal_agendor(lead, tag_type, product_type, user_id, company_id, token)


def updated_lead_agendor(lead, tag_type, product_type):

    # credencial no aws
    cred = CredencialApi()
    credentials = cred.get_agendor_token()
    
    token = credentials['token']

    ##
    ## BUSCAR USUARIO ###
    ##

    # Buscar usuario no agendor nos casos abaixo:

    #if tag_type == 'DIR' and product_type == 'Simples':

    # DIR=INTERNO | SAFIRA SIMPLES
    if tag_type == 'DIR':
        print('verificando user no simples interno...')
        user_id = get_user_id(lead['custom_fields']['[MKT] Responsável'], token)

    # EXT | FEI= EXTERNO FEIRA SAFRIA LIVER
    elif product_type == 'Livre' and tag_type in ['EXT', 'FEI']:
        print('verificando user no livre...')
        user_id = get_user_id(lead['custom_fields']['[Livre] Responsável'], token)

    # SAFIRA PADRÃO | SIMPLES
    else:
        print('verificando user no simples padrão...')
        user_id = 669907

    # NÃO HA PARA SAFIRA SOLAR | OUTRO CRM

    ##
    ## INICIO DO PROCESSO ###
    ##

    # Criar company no agendor usando token gerado
    print('Creating company...')

    company_id = create_company_agendor(lead, user_id, product_type, token)

    print(f'company id: {company_id}')

    # Criar pessoa no agendor usando token gerado
    print('Creating person...')    

    person_id = create_person_company(lead, user_id, company_id, token)

    print(f'person id: {person_id}')

    # Checar dados enviados foram criados no agendor usando token gerado
    check_deal_creation = check_if_can_create_lead(company_id, token)

    print(f"Can create: {check_deal_creation}")


    ##
    ## VALIDAÇÃO DOS DADOS - GERADOS COM SUCESSO NO AGENDOR ###
    ##

    # caso dados foram criados recber True, cria negociação associada ao user, company do agendor

    if check_deal_creation == True:
        # Create deal
        print('Creating deal...')
        
        # create_deal_agendor(lead, tag_type, product_type, user_id, company_id, token)


def get_user_id(user_name, token):
    
    # cred = CredencialApi()
    # credentials = cred.get_agendor_token()

    # token = credentials['token']

    # Create company
    print('Checking users...')

    url = f'https://api.agendor.com.br/v3/users'

    headers = {
        'Content-Type': 'application/json',
        'Authorization':f'Token {token}'
    }

    response = requests.get(url, headers=headers)

    users_json = response.json()
    users_str = json.dumps(users_json)
    users_str = json.loads(users_str)

    df = json_normalize(users_str['data'])
    
    id = int(df.loc[df['name'] == user_name, 'id'].iloc[0])

    return id

# checar dados 
#
def check_if_can_create_lead(org_id, token):
    
    url = f"https://api.agendor.com.br/v3/organizations/{org_id}/deals"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization':f'Token {token}'
    }

    response = requests.get(url, headers=headers)

    #response.json()
    
    r_json = response.json()
    r_str = json.dumps(r_json)
    r_str = json.loads(r_str)

    df = json_normalize(r_str['data'])
    
    # Start checking if it's okay to create the deal
    # Return True if its okay and False if its not okay
    
    if df.empty == True:
        return True
    
    else:
        current_time = datetime.datetime.now(timezone.utc)
        ten_min_ago = current_time - timedelta(minutes=10)

        results = []

        for date in df['createdAt'].tolist():

            time_to_check = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)

            results.append(ten_min_ago <= time_to_check <= current_time)

        if True in results:
            return False
        else:
            return True
        

def create_company_agendor(lead, user_id, product_type, token):

    # Create company
    print('Creating company...')

    url_company = f'https://api.agendor.com.br/v3/organizations/upsert'

    headers = {
        'Content-Type': 'application/json',
        'Authorization':f'Token {token}'
    }

    if product_type == 'Livre':
        payload_company = {
            "name": lead['company'],
            "cnpj": lead['custom_fields']['CNPJ'],
            # "address": {
            #     "state": lead['state']
            # },
            "contact": {
                "mobile": ''.join(filter(str.isdigit, lead['mobile_phone']))[2:]
            },
            "ownerUser": user_id
        }

    else:
        payload_company = {
            "name": lead['company'],
            "address": {
                "state": lead['state']
            },
            "contact": {
                "mobile": ''.join(filter(str.isdigit, lead['mobile_phone']))[2:]
            },
            "ownerUser": user_id
        }

    print(payload_company)

    try:
        response = requests.request("POST", url_company, headers=headers, json=payload_company)
        print(response.status_code)
        print(response.text)

    except Exception as e:
        print(e)

    company_id = response.json()['data']['id']

    print(f'company id: {company_id}')

    return company_id


def create_person_company(lead, user_id, company_id, token):

    #print('Creating person...')

    url_contact = f'https://api.agendor.com.br/v3/people/upsert'

    headers = {
        'Content-Type': 'application/json',
        'Authorization':f'Token {token}'
    }

    payload_contact = {
        "name": lead['name'],
        "organization": company_id,
        "contact": {
            "email": lead['email'],
            "mobile": ''.join(filter(str.isdigit, lead['mobile_phone']))[2:]
        },
        # "address": {
        #     "state": lead['state']
        # },
        # "customFields": {
        #     "empresa": lead['company']
        # },
        "ownerUser": user_id
    }

    print(payload_contact)

    try:
        response = requests.request("POST", url_contact, headers=headers, json = payload_contact)
        print(response.status_code)
        print(response.text)

    except Exception as e:
        print(e)

    person_id = response.json()['data']['id']

    print(f'person id: {person_id}')

    return person_id


def create_deal_agendor(lead, tag_type, product_type, user_id, company_id, token):

    # Create deal
    print('Creating deal...')
    url_deal = f'https://api.agendor.com.br/v3/organizations/{company_id}/deals'
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization':f'Token {token}'
    }

    counter = deals_counter()

    if product_type == 'Livre':
    # PRODUTO: LIVRE
        if '[MKT][Livre] OBS' in lead['custom_fields']:
            obs = lead['custom_fields']['[MKT][Livre] OBS']
        else:
            obs = ''

        if lead['custom_fields']['[MKT][Livre] seletor gestora'] == 'Sim':
            payload_deal = {
                "title": f'[{tag_type}] {counter} - {lead["company"]}',
                "description": obs,
                "dealStatusText": "ongoing",
                "funnel": 734911,
                "ownerUser": user_id,
                "dealStage": 1,
                "customFields": {
                    "gestora": lead['custom_fields']['[MKT][Livre] nome da gestora'],
                    "tag_type": tag_type,
                    "codigo": lead['uuid'],
                    "cf_cnpj": lead['custom_fields']['CNPJ']
                }
            }

        else:
            payload_deal = {
                "title": f'[{tag_type}] {counter} - {lead["company"]}',
                "description": obs,
                "dealStatusText": "ongoing",
                "funnel": 734911,
                "ownerUser": user_id,
                "dealStage": 1,
                "customFields": {
                    "tag_type": tag_type,
                    "codigo": lead['uuid'],
                    "cf_cnpj": lead['custom_fields']['CNPJ']

                }
            }

            

    # PRODUTO: SIMPLES
    else:
        if tag_type == 'PDN':
            payload_deal = {
                "title": f'[{tag_type}] {counter} - {lead["company"]}',
                "dealStatusText": "ongoing",
                #"funnel": 722455,
                "funnel": 660477,
                "dealStage": 3, # MQL
                "customFields": {
                    "parceiro": lead['custom_fields']['[MKT] Parceiro de Negócios'],
                    "valor_da_conta": lead['custom_fields']['[MKT] Valor da Conta'],
                    "cnae": lead['custom_fields']['[MKT] cnae'],
                    "tag_type": tag_type,
                    "codigo": lead['uuid'],
                    "cf_cnpj": lead['custom_fields']['CNPJ']

                }
            }

        elif tag_type == 'EXT':
            payload_deal = {
                "title": f'[{tag_type}] {counter} - {lead["company"]}',
                "dealStatusText": "ongoing",
                #"funnel": 722455,
                "funnel": 660477,
                "dealStage": 6, # SQL - Estudo de Viabilidade
                "customFields": {
                    "vendedor": lead['custom_fields']['[MKT] Vendedor'],
                    "valor_da_conta": lead['custom_fields']['[MKT] Valor da Conta'],
                    "cnae": lead['custom_fields']['[MKT] cnae'],
                    "tag_type": tag_type,
                    "codigo": lead['uuid'],
                    "cf_cnpj": lead['custom_fields']['CNPJ']

                }
            }

        elif tag_type == 'IND':
            payload_deal = {
                "title": f'[{tag_type}] {counter} - {lead["company"]}',
                "dealStatusText": "ongoing",
                #"funnel": 722455,
                "funnel": 660477,
                "dealStage": 3, # MQL
                "customFields": {
                    "indicado_por": lead['custom_fields']['[MKT] Indicador'],
                    "valor_da_conta": lead['custom_fields']['[MKT] Valor da Conta'],
                    "cnae": lead['custom_fields']['[MKT] cnae'],
                    "tag_type": tag_type,
                    "codigo": lead['uuid'],
                    "cf_cnpj": lead['custom_fields']['CNPJ']

                }
            }    
            
        elif tag_type == 'DIR':

            print(f"{lead['custom_fields']['[MKT] Responsável']} - {user_id}")

            if '[MKT] Indicador' in lead['custom_fields']:
                payload_deal = {
                    "title": f'[{tag_type}] {counter} - {lead["company"]}',
                    "dealStatusText": "ongoing",
                    #"funnel": 722455,
                    "funnel": 660477,
                    "ownerUser": user_id,
                    "dealStage": 6, # SQL - Estudo de Viabilidade
                    "customFields": {
                        "indicado_por": lead['custom_fields']['[MKT] Indicador'],
                        "valor_da_conta": lead['custom_fields']['[MKT] Valor da Conta'],
                        "cnae": lead['custom_fields']['[MKT] cnae'],
                        "tag_type": tag_type,
                        "codigo": lead['uuid'],
                        "cf_cnpj": lead['custom_fields']['CNPJ']

                    }
                }
                
            else:
                payload_deal = {
                    "title": f'[{tag_type}] {counter} - {lead["company"]}',
                    "dealStatusText": "ongoing",
                    #"funnel": 722455,
                    "funnel": 660477,
                    "ownerUser": user_id,
                    "dealStage": 6, # SQL - Estudo de Viabilidade
                    "customFields": {
                        "valor_da_conta": lead['custom_fields']['[MKT] Valor da Conta'],
                        "cnae": lead['custom_fields']['[MKT] cnae'],
                        "tag_type": tag_type,
                        "codigo": lead['uuid'],
                        "cf_cnpj": lead['custom_fields']['CNPJ']

                    }
                }
        
        elif tag_type == 'MKT':
            payload_deal = {
            "title": f'[{tag_type}] {counter} - {lead["company"]}',
            "dealStatusText": "ongoing",
            #"funnel": 722455,
            "funnel": 660477,
            "dealStage": 3, # MQL
            "customFields": {
                "valor_da_conta": lead['custom_fields']['[MKT] Valor da Conta'],
                "cnae": lead['custom_fields']['[MKT] cnae'],
                "tag_type": tag_type,
                "codigo": lead['uuid'],
                "cf_cnpj": lead['custom_fields']['CNPJ']

            }
        }
            
        elif tag_type == 'LST':
            payload_deal = {
            "title": f'[{tag_type}] {counter} - {lead["company"]}',
            "dealStatusText": "ongoing",
            #"funnel": 722455,
            "funnel": 660477,
            "dealStage": 1, # Lead
            "customFields": {
                "valor_da_conta": lead['custom_fields']['[MKT] Valor da Conta'],
                "cnae": lead['custom_fields']['[MKT] cnae'],
                "tag_type": tag_type,
                "codigo": lead['uuid'],
                "cf_cnpj": lead['custom_fields']['CNPJ']

            }
        }

        elif tag_type == 'FEI':
            payload_deal = {
            "title": f'[{tag_type}] {counter} - {lead["company"]}',
            "dealStatusText": "ongoing",
            #"funnel": 722455,
            "ownerUser": user_id,
            "funnel": 660477,
            "dealStage": 1, # Lead
            "customFields": {
                "nome_da_feira": lead['custom_fields']['[MKT] nome feira'],
                "valor_da_conta": lead['custom_fields']['[MKT] Valor da Conta'],
                "cnae": lead['custom_fields']['[MKT] cnae'],
                "tag_type": tag_type,
                "codigo": lead['uuid'],
                "cf_cnpj": lead['custom_fields']['CNPJ']

            }
        }

    try:
        response = requests.request("POST", url_deal, headers=headers, json = payload_deal)
        print(f'Lead on agendor response: {response.status_code} - {response.text}')
        print(response.status_code)
        print(response.text)

    except Exception as e:
        print(e)
        print(response.text)

def deals_counter():

    # GET CURRENT COUNT NUMBER

    boto_session = boto3.Session(region_name="us-east-1")
    s3_client = boto_session.client('s3')

    # Get the file inside the S3 Bucket
    s3_response = s3_client.get_object(
        Bucket='safira-pipeline-webhook',
        Key='agendor_deals_counter/counter.json'
    )

    # Get the Body object in the S3 get_object() response
    s3_object_body = s3_response.get('Body').read()

    # Read the data in bytes format
    #content = s3_object_body

    json_dict = json.loads(s3_object_body)
 
    count = json_dict['count']

    print(count)

    # ADD TO THE COUNTER

    json_dict['count'] = json_dict['count'] + 1

    s3_client.put_object(
        Body = json.dumps(json_dict), 
        Bucket='safira-pipeline-webhook', 
        Key='agendor_deals_counter/counter.json'
    )

    return count