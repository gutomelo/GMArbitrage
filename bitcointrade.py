# Script de inserção das cotações do BitcoinTrade

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
lista = {'BTC':'BRLBTC','ETH':'BRLETH','BCH':'BRLBCH','LTC':'BRLLTC','XRP':'BRLXRP','EOS':'BRLEOS','DAI':'BRLDAI'}

def bitcointrade():
    for i in lista:
        try:
            requisicao = requests.get('https://api.bitcointrade.com.br/v3/public/'+lista[i]+'/ticker', timeout=(1,1))
            if requisicao.status_code == 200:
                Symbol = requisicao.json()
            
                if 'data' in Symbol:
                    simbolo = i+'_bitcointrade'
                    bid = float(Symbol['data']['buy'])
                    ask = float(Symbol['data']['sell'])
                    volbid = 0
                    volask = 0
                    #timestamp = Symbol['data']['date']
                    timestamp = datetime.timestamp(datetime.now())
                    data = datetime.fromtimestamp(timestamp) 
                
                    cursor.execute(f"INSERT INTO {simbolo} (bid, ask, volbid, volask, timestamp) VALUES ({bid}, {ask}, {volbid}, {volask}, {timestamp})")
                    conn.commit()

                    if cursor.rowcount == 1:
                        if PRINT:
                            print(f'A cotação {simbolo} da BitCoinTrade foi inserido com sucesso, data {data}.')
                    else:
                        print(f'Não foi possível inserir a cotação {simbolo} da BitCoinTrade na data {data}.')
                
                    time.sleep(1.1)
            
        
        except KeyboardInterrupt:
            desconectar(conn)
            exit()
        
        except:
            continue
                
    
while True:
    bitcointrade()
        
    