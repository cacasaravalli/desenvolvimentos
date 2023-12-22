# O intuito deste script é subir no banco informações de um arquivo CSV

import pandas as pd
# import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote 
# import datetime

arq = 'C:/base_carteira_20231218.csv' #alterar caminho do arquivo csv

df_npa = pd.read_csv(arq
                     , parse_dates=['ref_fechamento']                        
                     , dtype = {'tipo_carteira':str
                                ,'fx_atraso':str
                                ,'carteira':str
                               }
                    )

try:
    conexao = create_engine('postgresql+psycopg2://carolini_saravalli:%s@xxxxxxxxxxxxxx.rds.amazonaws.com/segment_events' % quote('XXXXXXXXXX'))
except:
    print('Não foi possível conectar ao banco de dados. Checar conexão')


df_npa.to_sql(
    'rps_base_carteira_temp',
    con = conexao,
    schema = 'business_analytics',
    if_exists = 'replace',
    index = False,
    chunksize = 5000
)    

conexao.execute('GRANT ALL PRIVILEGES ON TABLE business_analytics.rps_base_carteira_temp to looker,business_analytics_role')