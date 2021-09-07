from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key

import json
import logging

import botocore
from datetime import datetime
from uuid import UUID
import decimal
import time

import dateutil.tz


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
        
        if 'queryStringParameters' in event and  event['queryStringParameters'] and 'Codigo_Pontuacao_Cliente' in event['queryStringParameters']:
    
            # Como o Lambda é serveless, não sei qual maquina de qual pais está rodando, preciso garantir que a data e hora seja de SP, no meu caso.
            timezoneSPBrazil = dateutil.tz.gettz('America/Sao_Paulo')
           
            data_e_hora_atuais =  datetime.now(tz=timezoneSPBrazil)
            data_e_hora_dia_ate_final = data_e_hora_atuais.strftime('%Y-%m-%d') 
            data_e_hora_dia_ate_final = data_e_hora_dia_ate_final + ':23:59:59'
            
            table = dynamodb.Table("Desconto")
            
            response = table.query(
                            IndexName="Pontuacao-Inicio-index",
                            Select="SPECIFIC_ATTRIBUTES",
                            ProjectionExpression="PercentualDesconto, DataFimDesconto, CodigoDesconto",
                            ConsistentRead=False,
                            ReturnConsumedCapacity="TOTAL",
                            KeyConditionExpression=Key('PontuacaoDoCliente').eq(int(event['queryStringParameters']['Codigo_Pontuacao_Cliente'])) & Key('DataInicioDesconto').lt(data_e_hora_dia_ate_final),)
                            
            # Altera encoder senao da erro para campos do tipo UUID, Decimal, Datetime, pois todos tem que virar string
            json.JSONEncoder.default = JSONEncoder_newdefault
            body = json.dumps(response)

        else:
            statusCode = 400
            body = 'Está faltando o query parametrer codigo_pesquisa_codigo'                

    elif rota == "GET /descontos/{id}":
        
        if 'pathParameters' in event and event['pathParameters'] and 'id' in event['pathParameters']:

            table = dynamodb.Table("Desconto")
            response = table.get_item(Key={'CodigoDesconto': event['pathParameters']['id']})
            
            if 'Item' in response and response['Item']:
                # Altera encoder senao da erro para campos do tipo UUID, Decimal, Datetime, pois todos tem que virar string
                json.JSONEncoder.default = JSONEncoder_newdefault
                body = json.dumps(response)
            else:
                statusCode = 404
                body = 'Nenhum registro encontrado'                


        else:
            statusCode = 400
            body = 'Está faltando o path parameter {id}'

    elif rota == "DELETE /descontos/{id}":
        
       if 'pathParameters' in event and event['pathParameters'] and 'id' in event['pathParameters']:

            table = dynamodb.Table("Desconto")
            response = table.delete_item(Key={'CodigoDesconto': event['pathParameters']['id']},
                                         ReturnValues='ALL_OLD')
            
            if 'Attributes' in response and response['Attributes']:
                # Altera encoder senao da erro para campos do tipo UUID, Decimal, Datetime, pois todos tem que virar string
                json.JSONEncoder.default = JSONEncoder_newdefault
                body = json.dumps(response)
            else:
                statusCode = 404
                body = 'Nenhum registro encontrado'                
        
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

# =============================================================================
# Altera o padrao encoder para evitar erros de parse no Python
# =============================================================================
def JSONEncoder_newdefault(self, o):
    if isinstance(o, UUID): return str(o)
    if isinstance(o, datetime): return str(o)
    if isinstance(o, time.struct_time): return datetime.fromtimestamp(time.mktime(o))
    if isinstance(o, decimal.Decimal): return str(o)
    return JSONEncoder_olddefault(self, o)
