# Script de inserção das cotações da NovaDax

import requests, MySQLdb , time
from datetime import datetime

# Função para conectar ao servidor
def conectar():
    try:
        conn = MySQLdb.connect(
            db='GMArbitrage',
            host='localhost',
            user='admin',
            passwd='123456'
        )
        return conn
    except MySQLdb.Error as e:
        print(f'Erro na conexão ao MySQL Server: {e}')


# Função para desconectar do servidor.
def desconectar(conn):
    if conn:
        conn.close()


conn = conectar()
cursor = conn.cursor()

PRINT = False # Para imprimir os logs de inserções
lista = {'BTC':'BTC_BRL','ETH':'ETH_BRL','BCH':'BCH_BRL','LTC':'LTC_BRL','XRP':'XRP_BRL','EOS':'EOS_BRL','DAI':'DAI_BRL'}

def novadax():
    try:
        requisicao = requests.get('https://api.novadax.com/v1/market/tickers',timeout=(1,1))
        if requisicao.status_code == 200:
            Pares = requisicao.json()
            if 'data' in Pares:
                Symbol = Pares['data']
            
                for i in Symbol:
                    if i['symbol'] in lista.values():
                        simbolo = i['symbol'][:3]+'_novadax'
                        bid = float(i['bid'])
                        ask = float(i['ask'])
                        volbid = 0
                        volask = 0
                        timestamp = i['timestamp']/1000  # tá em milisegundos
                        data = datetime.fromtimestamp(timestamp) 

                        cursor.execute(f"INSERT INTO {simbolo} (bid, ask, volbid, volask, timestamp) VALUES ({bid}, {ask}, {volbid}, {volask}, {timestamp})")
                        conn.commit()

                        if cursor.rowcount == 1:
                            if PRINT:
                                print(f'A cotação {simbolo} da NovaDax foi inserido com sucesso, data {data}.')
                        else:
                            print(f'Não foi possível inserir a cotação {simbolo} da NovaDax na data {data}')
                
                time.sleep(0.3)        
        
        
    except KeyboardInterrupt:
        desconectar(conn)
        exit()
    
    except:
        pass        


while True:
    novadax()
