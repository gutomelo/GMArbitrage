# Script de inserção das cotações do Mercado Bitcoin

import requests, MySQLdb, time
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
lista = {'BTC':'BTC','ETH':'ETH','BCH':'BCH','LTC':'LTC','XRP':'XRP'}

def mercadobitcoin():
    for i in lista:
        try:
            requisicao = requests.get('https://www.mercadobitcoin.net/api/'+lista[i]+'/ticker/',timeout=(1,1))
            if requisicao.status_code == 200:
                Symbol = requisicao.json()
            
                if 'ticker' in Symbol:
                    simbolo = i+'_mercadobitcoin'
                    bid = float(Symbol['ticker']['buy'])
                    ask = float(Symbol['ticker']['sell'])
                    volbid = 0
                    volask = 0
                    timestamp = Symbol['ticker']['date']
                    #timestamp = datetime.timestamp(datetime.now())
                    data = datetime.fromtimestamp(timestamp) 
                
                    cursor.execute(f"INSERT INTO {simbolo} (bid, ask, volbid, volask, timestamp) VALUES ({bid}, {ask}, {volbid}, {volask}, {timestamp})")
                    conn.commit()

                    if cursor.rowcount == 1:
                        if PRINT:
                            print(f'A cotação {simbolo} de MercadoBitcoin foi inserido com sucesso, data {data}.')
                    else:
                        print(f'Não foi possível inserir a cotação {simbolo} da MercadoBitcoin na data {data}.')
                
                    time.sleep(0.3)

        
        except KeyboardInterrupt:
            desconectar(conn)
            exit()
        
        except:
            continue        
        

while True:
    mercadobitcoin()
