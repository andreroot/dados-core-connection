import requests
from src.conectores.conecta_rdstation import CredencialApi

def create_chat_event(email, event_name):
    
    try:
        print('Chamada na api')

        cred = CredencialApi()
        token = cred.get_rdstation_token()
    
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": "Bearer " + token
        }

        url = "https://api.rd.services/platform/events"

        payload = {
            "event_type": "CONVERSION",
            "event_family": "CDP",
            "payload": {
                "conversion_identifier": event_name,
                "email": email
            }
        }

        response = requests.post(url, json=payload, headers=headers)
        print(f'Chat event response: {response.status_code} - {response.text}')

    # except JSONDecodeError as error:  # includes simplejson.decoder.JSONDecodeError
    #     print(f'Erro: {error} - {response.text}')
    #     print(response.status_code)

    except requests.exceptions.RequestException as error:  # This is the correct syntax
        print(f'Erro: {error} - {response.text}')
        print(response.status_code)


def email_exists(email):

    try:

        print('Verifiying lead existence:')

        #Check if already exists
        octa_email = email

        cred = CredencialApi()
        token = cred.get_rdstation_token()

        rd_check_url = f'https://api.rd.services/platform/contacts/email:{octa_email}'

        headers = {
            "accept": "application/json",
            "Authorization": "Bearer" + " " + token()
        }

        response = requests.get(rd_check_url, headers=headers)
        print(response)

        if response.status_code == 200:
            return True
        
        if response.status_code == 404:
            return False
        
        else:
            print(f'Erro: {response.text}')

    except requests.exceptions.RequestException as error:  # This is the correct syntax
        print(f'Erro: {error} - {response.text}')
        print(response.status_code)
