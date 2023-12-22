# O intuito deste código é criar a imagem correspondente à gráfico com base nos dados extraídos da query sql 

import pandas as pd
from os.path import join
import matplotlib.pyplot as plt

import aggregate_functions as af
import functions_graph as gf
from auxiliary_functions import *

retention_file = '09_01_retencao_mensal_segmentos.png'
activation_retention_file = '07_1_retencao_mensal.png'

def get_retention_data(conn):
    '''
    Query and process retention data for subsequent plotting
    Receives: connection to the DB'
    Returns: data ready for plotting
    '''

    #Query analytical data
    query_file_path = \
        r'C:/Users/CAROLINIDONASCIMENTO/OneDrive - Banqi/Documentos/RelatorioSemanal/user_retention.sql'

    query = open(query_file_path, 'r').read()
    data = pd.read_sql(query, conn)

    #Initial data preparation
    groupby_cols = ['mes_registro', 'flag_cdc', 'flag_ep', 'flag_cartao']
    agg_cols = ['Novas Contas', 0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    new_users = data.groupby(
        by=groupby_cols
    ).agg({'user_id': 'nunique'})

    active_users = data.groupby(
        by=groupby_cols + ['mes_ativo']
    ).agg({'user_id': 'nunique'}).reset_index()

    active_users = active_users.pivot(
        index=groupby_cols,
        columns='mes_ativo',
        values='user_id'
    )

    active_users = new_users.join(other=active_users)
    active_users.drop(columns=-1.0, inplace=True, errors='ignore')
    active_users.rename(columns={'user_id': 'Novas Contas'}, inplace=True)
    active_users.reset_index(inplace=True)

    #Group by the product cohorts
    cohorts = {
        'CDC': 'flag_cdc == True',
        'Cartão': 'flag_cartao == 1',
        'Empréstimo Pessoal': 'flag_ep == 1',
        'Nenhum Produto': 'flag_cdc == False and flag_cartao == 0 and flag_cartao == 0'
    }

    cohort_data = {}
    for cohort, filter in cohorts.items():
        aux_data = active_users.query(filter)
        aux_data = aux_data.groupby(by='mes_registro').agg(
            {col: 'sum' for col in agg_cols}
        )

        aux_data.columns = [
            col if type(col) == type('str') else f'M{col:.0f}'
            for col in aux_data.columns
        ]

        for col in aux_data.columns[1:]:
            aux_data[col] = 100 * aux_data[col] / aux_data['Novas Contas']

        aux_data.index = [
            convert_month_name(str(month)[:7]) for month in aux_data.index
        ]

        cohort_data.update({cohort: aux_data})

    return cohort_data

def plot_retention_tables(data, output_path):

    #Set up graph layout parameters
    size = (16, 8)
    dpi = 800
    file_name = join(output_path, retention_file)
    color_map = {
        'CDC': 'Blues_r',
        'Cartão': 'Greens_r',
        'Empréstimo Pessoal': 'GnBu_r',
        'Nenhum Produto': 'Reds_r'
    }

    #Create graph object
    fig, ax = plt.subplots(nrows=2, ncols=2, figsize=size)

    for i, (cohort, cohort_data) in enumerate(data.items()):
        row, col = divmod(i,2)
        show_index =  True if col == 0 else False

        table = gf.plot_retention_table(
            ax=ax[row,col],
            table_data=cohort_data,
            format_list=['{:,.0f}'] + ['{:,.1f}%'] * (len(cohort_data.columns)-1),
            cmap=color_map[cohort],
            index=show_index
        )
        gf.table_styling(ax=ax[row,col], table=table, fig=fig, title=cohort)

    plt.savefig(file_name, bbox_inches='tight', dpi=dpi)
    plt.close(fig)

def plot_activation_retention(conn, output_path):
    query = '''
    with usuarios_ativos as (
    select distinct 
        user_id,
        date_trunc('month', created_at) as mes_ativo
    from reporting.transactions_ext
    where 
        status = 'Complete' and 
        transaction_type not in (
            'AdminAdjustment', 
            'Bonus', 
            'Cashback', 
            'Charge', 
            'Fee', 
            'PixInReversal', 
            'PixOutReversal'))

    select distinct 
        atv.user_id,  
        to_char(date_trunc('month', atv.data_ativacao_conta), 'YYYY-MM') as mes_ativacao, 
        mes_ativo
    from 
        business_analytics.rps_ativacao_conta as atv
        inner join usuarios_ativos as tx on (atv.user_id = tx.user_id)
    where 
        date_trunc('month', atv.data_ativacao_conta) <  date_trunc('month', current_date) and 
        date_trunc('month', atv.data_ativacao_conta) >= date_trunc('month', current_date) - '13 months'::interval and
        mes_ativo <= (date_trunc('month', atv.data_ativacao_conta) + '3 months'::interval) and
        mes_ativo >= date_trunc('month', atv.data_ativacao_conta)
    '''
    data = pd.read_sql(query, conn)
    data['mes_retencao'] = [
        dif_months(begin, end) for begin, end in data[['mes_ativacao', 'mes_ativo']].values
    ]

    #Final data treatment
    data
    data = data.pivot_table(
        index='mes_ativacao', 
        columns='mes_retencao', 
        values='user_id',
        aggfunc='count'
    )
    data.columns = [f'M{col}' for col in data.columns]

    for col in data.columns[1:]:
        data[col] = 100 * data[col] / data['M0']
    
    data.drop(columns='M0', inplace=True)

    #Set up graph layout parameters
    size = (18, 7.5)
    legend_pos = (0.7, 0.08)
    dpi = 800
    file_name = join(output_path, activation_retention_file)

    color_map = {
        'M1': '#0b1e45',
        'M2': '#173f91',
        'M3': '#2666eb'
    }

    #Create graph
    fig, ax = plt.subplots(figsize=size)
    
    for col in data.columns:
        data_col = data[col].dropna()[:-1]
        x_data = [convert_month_name(month) for month in data_col.index]
        y_data = data_col.values

        #Plot monthly results
        gf.plot_line(
            ax=ax, x_data=x_data, y_data=y_data, color=color_map[col],
            label=f'Retenção {col}', line_width=2, marker='')

        #Set up annotations
        annotation_dict = {
            'x_data': x_data,
            'data': [y_data],
            'types': ['line'],
            'colors': [color_map[col]],
            'font_size': [11],
            'font_weight': ['normal'],
            'offset': [(-7,10)],
            'label_limit': 0.05,
            'format': '{:,.0f}%'
        }
        gf.annotate_graph(ax, nseries=1, **annotation_dict)

    #Style graph
    gf.apply_default_styling(ax)

    #Set up legend
    gf.get_legend(fig=fig, ncols=3, legend_pos=legend_pos)

    #Save figure
    plt.savefig(file_name, bbox_inches='tight', dpi=dpi)
    plt.close(fig)

def plot_user_retention(conn, output_path):
    data = get_retention_data(conn)
    plot_retention_tables(data, output_path)

    plot_activation_retention(conn, output_path)

