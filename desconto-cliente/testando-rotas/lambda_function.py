from pprint import pprint

import json
import logging


def lambda_handler(event, context):

    body = ""
    statusCode = 200

    # ========================================================
    # Chamada via API Gateway com proxy para Lambda
    # ========================================================
    
    if 'httpMethod' in event and 'resource' in event:
        rota = str(event['httpMethod']) + ' ' + str(event['resource'])
    else:
        statusCode = 400
        body = "Esta função Lambda deverá ser chamada via Proxy pelo API Gateway"
        return {
            'statusCode': statusCode,
            'body': body
        }        
    # ========================================================
    # Verifica rota a ser executada
    # ========================================================
    print(json.dumps(event))
    
    if rota == "PUT /descontos":

        body = 'Teste PUT'

    elif rota == "GET /descontos":
        
        body = 'Teste GET lista'

    elif rota == "GET /descontos/{id}":
        
        body = 'Teste GET por id'  

    elif rota == "DELETE /descontos/{id}":
        
        body = 'Teste DELETE por id' 
        
    elif rota == "PATCH /descontos/{id}":
        
        body = 'Teste PATCH por id' 
        
    else:
        statusCode = 500
        body = 'Rota nao suportada: ' + rota
        return {
            'statusCode': statusCode,
            'body': body
        }  

    return {
        'statusCode': statusCode,
        'body': body
        }

