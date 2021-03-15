# Script de inserção das cotações do BitPreço

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
lista = {'BTC':'btc-brl','ETH':'eth-brl'}

def bitpreco():
    for i in lista:
        try:
            requisicao = requests.get('https://api.bitpreco.com/'+lista[i]+'/ticker',timeout=(1,1))
            if requisicao.status_code == 200:
                Symbol = requisicao.json()
            
                if 'buy' and 'sell' in Symbol:
                    simbolo = i+'_bitpreco'
                    bid = float(Symbol['buy'])
                    ask = float(Symbol['sell'])
                    volbid = 0
                    volask = 0
                    timestamp = datetime.timestamp(datetime.strptime(Symbol['timestamp'], '%Y-%m-%d %H:%M:%S'))
                    data = datetime.fromtimestamp(timestamp)  
                
                    cursor.execute(f"INSERT INTO {simbolo} (bid, ask, volbid, volask, timestamp) VALUES ({bid}, {ask}, {volbid}, {volask}, {timestamp})")
                    conn.commit()

                    if cursor.rowcount == 1:
                        if PRINT:
                            print(f'A cotação {simbolo} da BitPreço foi inserido com sucesso, data {data}.')
                    else:
                        print(f'Não foi possível inserir a cotação {simbolo} da BitPreço na data {data}.')

                    time.sleep(0.3)     
                    
        except KeyboardInterrupt:
            desconectar(conn)
            exit()
                
        except:
            continue
        

while True:
    bitpreco()
    
    

