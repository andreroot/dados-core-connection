import requests
import json

from src.conectores.conecta_octadesk import CredencialApi, get_secret
from json.decoder import JSONDecodeError

# def get_credentials():

#     region = os.getenv("AWS_DEFAULT_REGION")

#     boto_session = boto3.Session(
#         region_name=region,
#     )

#     secret_name = "octadesk-credentials"
   
#     client = boto_session.client(
#         service_name='secretsmanager',
#         region_name=region 
#     )

#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
        
#     except ClientError as e:
#         # For a list of exceptions thrown, see
#         # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
#         raise e

#     # Decrypts secret using the associated KMS key.
#     secret = get_secret_value_response['SecretString']

#     # Your code goes here.
#     # json.loads(secret)
#     return secret

# def get_octadesk_token():
    
#     try:
#         cred = get_credentials()

#         credentials = json.loads(cred)
        
#         print("credencial octadesk:",credentials)

#         username = credentials['username']
#         apiToken = credentials['apiToken']
#         subdomain = credentials['subdomain']

#         # Getting token
#         url = 'https://api.octadesk.services/login/apiToken'

#         headers = {
#             'Content-Type':'application/json',
#             'subDomain': subdomain,
#             'username':username,
#             'apiToken': apiToken
#         }

#         response = requests.request("POST",url, headers=headers)
        
#         token = response.json()['token']

#         return token

#     except requests.exceptions.RequestException as error:  # This is the correct syntax
#         print(f'Erro: {error} - {response.text}')
#         print(response.status_code)
    
    
        
def insert_octadesk(lead, tag_type):

    try:
        print('Chamada na api')

        cred = CredencialApi()
        token = cred.get_octadesk_token()

        #token = get_octadesk_token()

        url = 'https://api.octadesk.services/persons/'

        headers = {
            'accept': 'application/json',
            'Content-Type':'application/json',
            'Authorization': f'Bearer {token}'
        }
        #{token}
        #insert payload octadesk:{'email': 'andre.barbosa@gpsafira.com.br', 'name': 'andre barbosa', 'customField': {'valor_da_conta': 'Entre R$ 150,00 e R$ 499,99', 'distribuidora': 'MG - Cemig', 'tag_octadesk': 'formulario_solar'}, 'phoneContacts': [{'number': '11940147165', 'countryCode': '55', 'type': 1}]}

        payload = {
            "email": lead['email'],
            "name": lead['name'],
            "customField": {
                "solar_valor_conta": lead['custom_fields']['[MKT] Valor Conta'],
                "solar_distribuidora": lead['custom_fields']['[MKT] Distribuidora'],
                "tag_octadesk": lead['custom_fields']['[MKT] tag octadesk']
            },
            "phoneContacts": [
                {
                    "number": ''.join(c for c in lead['personal_phone'] if c.isdigit())[2:],
                    "countryCode": ''.join(c for c in lead['personal_phone'] if c.isdigit())[:2],
                    "type": 1
                }
            ]
        }
        
        print(f'insert payload octadesk:{payload}')

        response = requests.post(url, data=json.dumps(payload), headers=headers)

        print(response.status_code)
        print(response.text)

    # except Exception as e:
    #     print(f'Erro: {e}')

    except JSONDecodeError as error:  # includes simplejson.decoder.JSONDecodeError

        print(f'Erro: {error} - {response.text}')
        print(response.status_code)

    except requests.exceptions.RequestException as error:  # This is the correct syntax
        print(f'Erro: {error} - {response.text}')
        print(response.status_code)


    
def send_message(lead, template_id):

    try:
        # Treating phone number chars
        # Remove non-numeric characters
        mobile_phone = f"+{''.join(c for c in lead['personal_phone'] if c.isdigit())}"

        # # Getting Whatsapp API Key
        cred = get_secret()
        credentials = json.loads(cred)

        whats_key = credentials['safira-key']

        url = "https://o173699-c58.api001.octadesk.services/chat/conversation/send-template"

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "X-API-KEY": whats_key
        }

        payload = {
            "target": {
                "contact": { 
                    "phoneContact": { 
                        "number": mobile_phone 
                    },
                    "customFields": [
                        {
                            "type": "person",
                            "value": lead['name'],
                            "key": "nome-contato"
                        }
                    ]
                } 
            },
            "content": {
                "templateMessage": {
                    "id": template_id 
                } 
            },
            "origin": {
                "from": {
                    "number": "+558000723472" 
                } 
            },
            "options": { 
                "automaticAssign": False 
            }
        }

        print(f'insert payload octadesk:{payload}')

        #data=json.dumps(payload)

        response = requests.post(url, json=payload, headers=headers)
        print(response.status_code)
        print(response.text)

    except JSONDecodeError as error:  # includes simplejson.decoder.JSONDecodeError

        print(f'Erro: {error} - {response.text}')
        print(response.status_code)

    except requests.exceptions.RequestException as error:  # This is the correct syntax
        print(f'Erro: {error} - {response.text}')
        print(response.status_code)



    # try:


    # except Exception as e:
    #     print(f'Erro: {e}')

    # print(response.status_code)

