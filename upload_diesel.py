import pandas as pd
import numpy as np
import sys, os
import cx_Oracle
import datetime as dt

class Style:
    HEADER = lambda i: '\33[30m'+'\33[46m'+str(i)+'  '+'\33[0m'
    COMPLEMENT = lambda i: '\033[31m'+str(i)+'\33[0m'
    COMPLEMENT2 = lambda i: '\33[36m'+str(i)+'\33[0m'
    RESET = lambda i: '\33[0m'+str(i)

class UpdateDieselOutput:
    def __init__(self):
        self.project_variables = os.environ
        self.dsnStr = cx_Oracle.makedsn(self.project_variables['SERVER_LATAM'], self.project_variables['SERVER_LATAM_HOST'], self.project_variables['SERVER_LATAM_SID'])
        self.conn = cx_Oracle.connect(user=self.project_variables['SERVER_LATAM_NAME'], password=self.project_variables['SERVER_LATAM_PASSWORD'], dsn=self.dsnStr)
        self.hoje = dt.date.today()

    def load_table(self, link1):
        tbl1=pd.read_csv(link1, sep=";").fillna(0)
        tbl1['DATA_INICIAL']=pd.to_datetime(tbl1['DATA_INICIAL'])
        dict_sequence=[{
            '1': self.hoje, '2':tbl1['DATA_INICIAL'][i], '3':tbl1['ESTADO'][i], '4':tbl1['MUNICIPIO'][i], '5':tbl1['PRODUTO'][i], '6':tbl1['PRECO_MEDIO_DE_REVENDA'][i],
            '7': tbl1['PRECO_MINIMO_REVENDA'][i], '8':tbl1['PRECO_MAXIMO_REVENDA'][i], '9':tbl1['PRECO_MEDIO_DE_DISTRIBUICAO'][i], '10':tbl1['PRECO_MINIMO_DSITRIBUICAO'][i],
            '11':tbl1['PRECO_MAXIMO_DISTRIBUICAO'][i]
            } for i in range(0, tbl1.shape[0])]
        return dict_sequence

    def upload_data(self, link1):
        sql_insert='INSERT INTO DIESEL_BRASIL_OUTPUT '\
                   '(TRADING_DATE, DATA_INICIAL, ESTADO, MUNICIPIO, PRODUTO, PRECO_MEDIO_DE_REVENDA, PRECO_MINIMO_REVENDA, PRECO_MAXIMO_REVENDA, '\
                   'PRECO_MEDIO_DE_DISTRIBUICAO, PRECO_MINIMO_DSITRIBUICAO, PRECO_MAXIMO_DISTRIBUICAO) '\
                   'VALUES (:1, :2,:3, :4, :5, :6, :7, :8, :9, :10, :11)'

        dict_sequence2=self.load_table(link1)
        print(Style.HEADER('Tabela Carregada!!'))
        c=self.conn.cursor()
        c.prepare(sql_insert)
        c.executemany(None, dict_sequence2)
        self.conn.commit()
        print(Style.HEADER('Dados Upados!!'))

if __name__=='__main__':
    UpdateDieselOutput().upload_data(link1=dir_path.replace('\\', '/') + '/tabelaCompleta_Diesel_Jull20.py')

class CalculateAverages:
    def __init__(self, link1):
            self.tbl1 = pd.read_csv(link1, sep=";").fillna(0)
            self.tbl1['DATA_INICIAL'] = pd.to_datetime(self.tbl1['DATA_INICIAL'])

    @staticmethod
    def generate_monthly(input, directory1, product):
        input['DATA_INICIAL'] = pd.to_datetime([x.strftime('%Y-%m-01') for x in input['DATA_INICIAL']])
        diesel_comum_monthly = input.set_index(['DATA_INICIAL'])
        diesel_comum_monthly_avg = diesel_comum_monthly.groupby([diesel_comum_monthly.index, 'ESTADO', 'MUNICIPIO', 'PRODUTO']).mean().fillna(0).sort_index().reset_index()
        diesel_comum_monthly_avg.to_csv(directory1 + '/diesel_monthly_' + product + '.csv', sep=';', index=True)
        print(Style.COMPLEMENT2(f'Monthly Calculation for {product} is OK!'))
        diesel_comum_monthly_avg2 = diesel_comum_monthly.groupby([diesel_comum_monthly.index, 'PRODUTO']).mean().fillna(0).sort_index()
        diesel_comum_monthly_avg2.to_csv(directory1 + '/diesel_monthly_avg_' + product + '.csv', sep=';', index=True)
        print(Style.COMPLEMENT2(f'Monthly Average Calculation for {product} is OK!'))

    def generate_weekly(self, directory1):
        for i in ['OLEO_DIESEL', 'OLEO_DIESEL_S10']:
            diesel_weekly = self.tbl1[self.tbl1['MUNICIPIO'].isin(['UBERLANDIA', 'GOIANIA', 'ITUMBIARA', 'RIOVERDE', 'PAULINIA', 'ILHEUS', 'CASTRO', 'BARREIRAS']) &
                                            self.tbl1['PRODUTO'].isin([i])].loc[:,
                                ['DATA_INICIAL', 'ESTADO', 'MUNICIPIO', 'PRODUTO',
                                 'PRECO_MEDIO_DE_REVENDA', 'PRECO_MEDIO_DE_DISTRIBUICAO']].groupby\
                (['DATA_INICIAL', 'ESTADO', 'MUNICIPIO', 'PRODUTO']).apply(pd.DataFrame).reset_index(0).drop(columns=['index'])
            diesel_weekly.to_csv(directory1 + '/diesel_weekly_' + i + '.csv', sep=';', index=True)
            print(Style.COMPLEMENT2(f'Weekly Calculation for {i} is OK!'))
            self.generate_monthly(input=diesel_weekly, directory1=directory1, product=i)

            diesel_weekly_avg = self.tbl1[self.tbl1['MUNICIPIO'].isin(['UBERLANDIA', 'GOIANIA', 'ITUMBIARA', 'RIOVERDE', 'PAULINIA', 'ILHEUS', 'CASTRO', 'BARREIRAS']) &
                                            self.tbl1['PRODUTO'].isin([i])].loc[:,
                                ['DATA_INICIAL', 'PRODUTO', 'PRECO_MEDIO_DE_REVENDA', 'PRECO_MEDIO_DE_DISTRIBUICAO']].groupby(['DATA_INICIAL', 'PRODUTO']).mean().fillna(0).sort_index()
            diesel_weekly_avg.to_csv(directory1 + '/diesel_weekly_avg_' + i + '.csv', sep=';', index=True)
            print(Style.COMPLEMENT2(f'Weekly Average Calculation for {i} is OK!'))

if __name__=='__main__':
    CalculateAverages(link1=dir_path.replace('\\', '/') + '/tabelaCompleta_Diesel_Jull20.py').generate_weekly(directory1=dir_path.replace('\\', '/'))

