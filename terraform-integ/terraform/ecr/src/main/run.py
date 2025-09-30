from src.main.authorization import authorization

import urllib.parse
import json
import requests
import re
# import pandas as pd 
# from pandas import json_normalize
from src.octadesk.webhook import insert_octadesk, send_message

from src.rdstation.webhook import create_chat_event
from src.agendor.webhook import create_lead_agendor, updated_lead_agendor

from src.ibridge.webhook import insert_ibridge


def lambda_handler(event, context):

    try:

        # RECEBE DO EVENTO 
        print(f'event: {event}')

        # EXTRAIR ELEMENTOS DO JSON rawQueryString
        raw_query_string = event.get('rawQueryString', '')
        parsed_query = urllib.parse.parse_qs(raw_query_string)
        
        # EXTRAIR PARAMETROS: EVENT
        event_label = parsed_query.get('event', [''])[0]

        # PRNT DO EVENTO
        print(f'event_label: {event_label}')

        # EXTRAIR PARAMETROS: TOKEN
        token = parsed_query.get('token', [''])[0]


        # APLICAR AUTORIZAÇÃO VIA TOKEN RECEBIDO
        if not authorization.Authorization.authorize(token):
            print('Invalid authorization token')
            return {
            "statusCode": 401,
            "headers": {
                "Content-Type": "text/plain"
            },
            "body": 'Invalid authorization token'
            }
        
        # Cria o lead no Octadesk src/octadesk/webhook/insert_octadesk
        if event_label == 'new_lead':
            
            print('Inserindo lead:')

            tag_type = parsed_query.get('tag_type', [''])[0]
            # PRNT DO TAG TYPE
            print(f'tag_type: {tag_type}')

            # TRATAR EVENTO EXTRAIR LEAD
            body_str = event.get('body', '')
            body = json.loads(body_str)
            lead = body['leads'][0]
            # PRNT DO LEAD
            print(f'lead: {lead}')

            insert_octadesk(lead, tag_type)

        # Envia uma mensagem automatica para o lead (pelo octadesk) src/octadesk/webhook/send_message
        if event_label == 'send_message':
            
            print('Enviando mensagem:')

            # TRATAR EVENTO EXTRAIR LEAD
            body_str = event.get('body', '')
            body = json.loads(body_str)
            lead = body['leads'][0]

            # PRNT DO LEAD
            print(f'lead: {lead}')

            templateid = parsed_query.get('templateId', [''])[0]
            print(templateid)

            template_id = parsed_query.get('templateId', [''])[0]
            send_message(lead, template_id)

        # Cria lead no Agendor src/agendor/webhook/create_lead_agendor
        if event_label == 'whatschat_redirected':

            # EXTRAIR PARAMETROS: EMAIL
            octa_email = parsed_query.get('Email', [''])[0]

            # octa_email = body['Email']
            print(f'Redirecionando msg para email:{octa_email}')

            # TRATAR INFORMAÇÕES PARA CRIAR CHAT
            create_chat_event(octa_email, event_label)

            # # criar nova conversa: contato criado
            # print('Creating conversion event:')
            # create_chat_event(octa_email, f"{body['bot_type']}chat_started")

            # # encerrar conversa: contato ja existente
            # print('Finished conversion event:')
            # create_chat_event(octa_email, f"{body['bot_type']}chat_finished")

        # # Cria lead no Agendor src/agendor/webhook/create_lead_agendor        
        if event_label == 'send_to_agendor':
            
            print('Inserindo lead no agendor:')

            tag_type = parsed_query.get('tag_type', [''])[0]
            # PRNT DO TAG TYPE
            print(f'tag_type: {tag_type}')

            # TRATAR EVENTO EXTRAIR LEAD
            body_str = event.get('body', '')
            body = json.loads(body_str)
            lead = body['leads'][0]
            # PRNT DO LEAD
            print(f'lead: {lead}')

            # - caso tenha tag no payload recebido:
            # product_type = parsed_query.get('product', [''])[0]

            # no caso do agendor forçar produto Simples
            product_type = 'Simples'
            print(f'Produto: {product_type}')

            create_lead_agendor(lead, tag_type, product_type)

        # # Cria lead no Agendor src/agendor/webhook/create_lead_agendor        
        if event_label == 'send_to_ibridge':

            print('Inserindo lead no ibridge:')

            # TRATAR EVENTO EXTRAIR DADOS DO LEAD DO AGENDOR SOMENTE PAP
            body_str = event.get('body', '')
        
            body = json.loads(body_str)
            
            # VALIDAR SE CONTATO LEAD DO AGENDOR É UM PAP
            pap = body['data']['title']
            # lead = body['data']
            valid_pap = re.search(r"[PAP]\w+",pap)
            
            print(valid_pap.group(0))
            
            if valid_pap.group(0)=='PAP':
                print(f'{pap} - ok')
                insert_ibridge(body)

            else:
                print(f'{pap} - nok')
                        


        # # Cria lead no Agendor src/agendor/webhook/create_lead_agendor        
        if event_label == 'update_to_agendor':

            print('Updated lead/deals no agendor:')

            # tag_type = parsed_query.get('tag_type', [''])[0]
            # # PRNT DO TAG TYPE
            # print(f'tag_type: {tag_type}')

            # TRATAR EVENTO EXTRAIR LEAD
            body_str = event.get('body', '')
            body = json.loads(body_str)
            lead = body['leads'][0]
            # PRNT DO LEAD
            print(f'lead: {lead}')

            # - caso tenha tag no payload recebido:
            # product_type = parsed_query.get('product', [''])[0]

            # no caso do agendor forçar produto Simples
            product_type = 'Simples'
            print(f'Produto: {product_type}')
            tag_type = ''

            updated_lead_agendor(lead, tag_type, product_type)  
                                                              
        return {"statusCode": 200}

    except Exception as e:
        print(e)
        return {"statusCode": 400}

