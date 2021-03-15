# Esse script verifica se as cotações foram atualizadas nos últimos 10 segundos
# e mostra o status na tela

import MySQLdb, time, os
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
#pares = ['BTC','ETH','BCH','LTC','XRP','EOS','DAI']

bitcoin = ['BTC_novadax', 'BTC_bitcointrade', 'BTC_binance', 'BTC_mercadobitcoin', 'BTC_bitpreco', 'BTC_bitcointoyou']
ethereum = ['ETH_novadax', 'ETH_bitcointrade', 'ETH_binance', 'ETH_mercadobitcoin', 'ETH_bitpreco', 'ETH_bitcointoyou']
bcash = ['BCH_novadax', 'BCH_bitcointrade', 'BCH_mercadobitcoin'] 
litecoin = ['LTC_novadax', 'LTC_bitcointrade', 'LTC_mercadobitcoin', 'LTC_bitcointoyou']
xrp = ['XRP_novadax', 'XRP_bitcointrade', 'XRP_binance', 'XRP_mercadobitcoin']
eos = ['EOS_novadax', 'EOS_bitcointrade']
dai = ['DAI_novadax', 'DAI_bitcointrade']

pares = [bitcoin, ethereum, bcash, litecoin, xrp, eos, dai]

def status():
    for i in pares:
        for y in i:
            try:
                cursor.execute(f'SELECT timestamp FROM GMArbitrage.{y} where id{y[:3]} = (SELECT MAX(id{y[:3]}) FROM GMArbitrage.{y});')
                maxbtc = cursor.fetchall()
                if len(maxbtc) > 0:
                    now = datetime.timestamp(datetime.now())
                    data = maxbtc[0][0]
                    if now - data < 10:
                        print(f'Ativo {y}: OK')
                    else:
                        print(f'Ativo {y}: OFF')    
                conn.commit()
            
            except KeyboardInterrupt:
                desconectar(conn)
                exit()
     

while True:
    os.system('clear') or None
    status()
    time.sleep(1)