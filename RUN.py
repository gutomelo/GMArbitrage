# Esse Script executa todos os outros para a inserção das cotações no Banco de Dados MySQL de modo paralelo
# usando a biblioteca multiprocessing.

import os                                                                       
from multiprocessing import Pool                                                
                                                                                
dir_path = os.path.dirname(os.path.realpath(__file__))

processos = ('novadax.py', 'bitcointrade.py', 'binance.py', 'mercadobitcoin.py', 'bitpreco.py', 'bitcointoyou.py')                                    
                                                  
def roda_processo(processo):                                                             
    arquivo = dir_path+'/'+processo
    os.system(f'python {arquivo}')                                       
                                                                                
pool = Pool(processes=6)                                                        
pool.map(roda_processo, processos) 

