

##http://safiracallcenter-crm.ibridge.net.br/api/v2/?k=244d7acff356011b3fae8201526c9463&m=contatos&a=adicionar&operacao_id=1&campanha_id=1&lista_id=1&contato_codigo=123&contato_nome=Teste&contato_telefone_1=99999999999&contato_email=teste@gmail.com&modo=carteira&contato_codigo_unico=true&contato_estado=1&chamada_retorno=-1
##ibridge-credentials

import requests
import json

from src.conectores.conecta_ibridge import get_secret
from json.decoder import JSONDecodeError

def insert_ibridge(lead):

    try:
        print('Chamada na api')

        # cred = CredencialApi()
        tokenx = get_secret() #cred.get_ibridge_api()
        tokenx = json.loads(tokenx)
        
        token = tokenx['key']
        
        cod_empresa = lead['data']['person']['id']
        nome_empresa = lead['data']['person']['organization']['name']
        decisor = lead['data']['person']['name']
        tel = lead['data']['person']['contact']['work']
        email = lead['data']['person']['contact']['email']
        etapa = lead['data']['dealStage']['name']
        funil = lead['data']['dealStage']['funnel']['name']
        
        #token = get_octadesk_token()

        url = 'http://safiracallcenter-crm.ibridge.net.br/api/v2/?'

        headers = {
            'accept': 'application/json',
            'Content-Type':'application/json'
        }
        #{token}
        #insert payload octadesk:{'email': 'andre.barbosa@gpsafira.com.br', 'name': 'andre barbosa', 'customField': {'valor_da_conta': 'Entre R$ 150,00 e R$ 499,99', 'distribuidora': 'MG - Cemig', 'tag_octadesk': 'formulario_solar'}, 'phoneContacts': [{'number': '11940147165', 'countryCode': '55', 'type': 1}]}

        url += f'k={token}&m=contatos&a=adicionar&operacao_id=1&campanha_id=1&lista_id=97&contato_codigo={cod_empresa}'
        url += f'&contato_nome={nome_empresa}&contato_telefone_1={tel}&contato_email={email}&contato_decisor_1={decisor}&modo=carteira&contato_codigo_unico=true&chamada_retorno=-1'
        
        print(f'insert payload ibridge:{url}')

        response = requests.post(url, headers=headers)

        print(response.status_code)
        print(response.text)
    except JSONDecodeError as error:  # includes simplejson.decoder.JSONDecodeError

        print(f'Erro: {error} - {response.text}')
        print(response.status_code)

    except requests.exceptions.RequestException as error:  # This is the correct syntax
        print(f'Erro: {error} - {response.text}')
        print(response.status_code)

