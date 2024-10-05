import pendulum
import json
import requests
from datetime import datetime

def extracao_cotacao_dolar_bacen(data_interval_start, **kwargs):    
    # Data dinamica e URL para realizar o get dos dados
    data = pendulum.parse(data_interval_start).strftime("%m-%d-%Y")
    url = f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{data}'&$top=100&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
    
    # Requisição da API
    response = requests.get(url=url)

    if response.status_code == 200:
        print(f"Sucesso na requisição - {response}")
    else:
        print(f"Falha na requisição {response}")
    
    arquivo_json = response.json()
    
    print(f"\nDados extraídos na data: {datetime.now()}\n"), \
    print(json.dumps(arquivo_json, indent=4))

    return arquivo_json