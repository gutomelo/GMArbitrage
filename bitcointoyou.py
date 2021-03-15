# Script de inserção das cotações do BitcoinToYou

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
lista = {'BTC':'BTC_BRLC','ETH':'ETH_BRLC','LTC':'LTC_BRLC'}

def bitcointoyou():
    for i in lista:
        try:
            requisicao = requests.get('https://back.bitcointoyou.com/API/orderbook?pair='+lista[i]+'&depth=1',timeout=(1,1))
            if requisicao.status_code == 200:
                Symbol = requisicao.json()
            
                if 'bids' and 'asks' in Symbol:
                    simbolo = i+'_bitcointoyou'
                    bid = float(Symbol['bids'][0][0])
                    ask = float(Symbol['asks'][0][0])
                    volbid = float(Symbol['bids'][0][1])
                    volask = float(Symbol['asks'][0][1])
                    timestamp = datetime.timestamp(datetime.now())
                    data = datetime.fromtimestamp(timestamp)  
                
                    cursor.execute(f"INSERT INTO {simbolo} (bid, ask, volbid, volask, timestamp) VALUES ({bid}, {ask}, {volbid}, {volask}, {timestamp})")
                    conn.commit()

                    if cursor.rowcount == 1:
                        if PRINT:
                            print(f'A cotação {simbolo} da BitcoinToYoy foi inserido com sucesso, data {data}.')
                    else:
                        print(f'Não foi possível inserir a cotação {simbolo} da BitcoinToYoy na data {data}.')
                
                    time.sleep(0.3)
            
        
        except KeyboardInterrupt:
            desconectar(conn)
            exit()
        
        except:
            continue
        
 
while True:
    bitcointoyou()
 
