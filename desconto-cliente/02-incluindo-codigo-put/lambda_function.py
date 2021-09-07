
from pprint import pprint

import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

import json
import logging

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):

    global dynamodb

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
    eventoAjustado = json.dumps(event)
    print(eventoAjustado)
    
    if rota == "PUT /descontos":

        if 'body' in event:
            
            #Transforma objeto em Json Sting
            jsonAjustado = json.dumps(event['body'])
            
            #Transforma Json String em Objeto Python com scapes
            jsonPut = json.loads(jsonAjustado)
            
            #Transforma objeto com scapes em objeto Python correto 
            jsonPut = json.loads(jsonPut)
            
            
            table = dynamodb.Table("Desconto")
            table.put_item(
                Item={
                    'CodigoDesconto': jsonPut['codigo_desconto'],
                    'DataInicioDesconto': jsonPut['data_inicio_desconto'],
                    'DataFimDesconto': jsonPut['data_fim_desconto'],
                    'PontuacaoDoCliente': jsonPut['pontuacao_do_cliente'],
                    'PercentualDesconto': jsonPut['percentual_desconto']})
                    
            body = 'Put Item ' + jsonPut['codigo_desconto']
        else:
            statusCode = 500
            body = 'PUT sem objeto body'
            return {
                'statusCode': statusCode,
                'body': body
            }   

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

