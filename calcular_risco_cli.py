# Arquivo: calcular_risco_cli.py
import os
import sys
import requests
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta # Importamos timedelta
from pytz import timezone


URL_ARQUIVO_HISTORICO = 'https://raw.githubusercontent.com/RafaellaB/Painel-Diagrama-de-Risco/main/resultado_risco_final.csv'
URL_ARQUIVO_MARE_AM = 'https://raw.githubusercontent.com/RafaellaB/Diagramas-de-risco-din-mico/main/tide/mare_calculada_hora_em_hora_ano-completo.csv'
NOME_ARQUIVO_SAIDA_FINAL = 'resultado_risco_final.csv'
CSV_DELIMITADOR = ',' 
ESTACOES_DESEJADAS = ["Campina do Barreto", "Torreão", "RECIFE - APAC", "Imbiribeira", "Dois Irmãos"]


def carregar_dados_mare(url_am_data):
    """ Carrega o arquivo de maré (AM) ANUAL. """
    try:
        df_am_raw = pd.read_csv(url_am_data)
        df_am_raw.rename(columns={'datahora': 'datahora', 'altura': 'AM'}, inplace=True)
        df_am_raw['datahora'] = pd.to_datetime(df_am_raw['datahora'])
        df_am_raw['data'] = df_am_raw['datahora'].dt.strftime('%Y-%m-%d')
        df_am_raw['hora_ref'] = df_am_raw['datahora'].dt.strftime('%H:00:00')
        return df_am_raw[['data', 'hora_ref', 'AM']]
    except Exception as e:
        print(f"ERRO: Falha ao carregar dados de maré: {e}", file=sys.stderr)
        return pd.DataFrame()

def processar_dados_chuva_simplificado(df_chuva, datas_desejadas, estacoes_desejadas):
    """ Calcula o indicador horário de chuva 'VP'. """
    df = df_chuva[df_chuva['nomeEstacao'].isin(estacoes_desejadas)].copy()
    df['data'] = df['datahora'].dt.date.astype(str)
    df = df[df['data'].isin(datas_desejadas)]
    if df.empty: return pd.DataFrame()
    df = df.set_index('datahora').sort_index()
    resultados_por_estacao = []
    for estacao, grupo in df.groupby('nomeEstacao'):
        chuva_10min = grupo['valorMedida'].rolling('10min').sum()
        chuva_2h = grupo['valorMedida'].rolling('2h').sum()
        temp_df = pd.DataFrame({'chuva_10min': chuva_10min, 'chuva_2h': chuva_2h})
        agregado_horario = temp_df.resample('h').last()
        agregado_horario['VP'] = (agregado_horario['chuva_10min'] * 6) + agregado_horario['chuva_2h']
        agregado_horario['nomeEstacao'] = estacao
        resultados_por_estacao.append(agregado_horario)
    df_vp = pd.concat(resultados_por_estacao).reset_index()
    df_vp.dropna(subset=['VP'], inplace=True)
    df_vp['data'] = df_vp['datahora'].dt.strftime('%Y-%m-%d')
    df_vp['hora_ref'] = df_vp['datahora'].dt.strftime('%H:00:00')
    return df_vp[['data', 'hora_ref', 'nomeEstacao', 'VP']]

def calcular_risco(df_final):
    """ Calcula o Nível de Risco (VP * AM) e a Classificação. """
    if df_final.empty: return pd.DataFrame()
    df_final['VP'] = pd.to_numeric(df_final['VP'], errors='coerce').round(2) 
    df_final['AM'] = pd.to_numeric(df_final['AM'], errors='coerce').round(2)
    df_final['Nivel_Risco_Valor'] = (df_final['VP'] * df_final['AM']).fillna(0).round(2)
    bins = [-np.inf, 30, 50, 100, np.inf]
    labels = ['Baixo', 'Moderado', 'Moderado Alto', 'Alto']
    df_final['Classificacao_Risco'] = pd.cut(df_final['Nivel_Risco_Valor'], bins=bins, labels=labels, right=False)
    return df_final

def executar_analise_risco_completa(df_vp_calculado, df_am):
    """ Mescla VP e AM e chama o cálculo de risco. """
    if df_vp_calculado.empty: return pd.DataFrame()
    df_final = pd.merge(df_vp_calculado, df_am, on=['data', 'hora_ref'], how='left')
    df_risco = calcular_risco(df_final)
    return df_risco

if __name__ == "__main__":
    
 
    tz_recife = timezone('America/Recife') 
   
    data_para_processar = datetime.now(tz_recife).date() - timedelta(days=1)
    data_para_processar_str = data_para_processar.strftime('%Y-%m-%d')
    
 
    nome_arquivo_chuva = f"chuva_recife_{data_para_processar_str}.csv"
    
    print(f"Iniciando cálculo de risco para a data: {data_para_processar_str}")
    
    if not os.path.exists(nome_arquivo_chuva):
        print(f"❌ ERRO: Arquivo de chuva '{nome_arquivo_chuva}' não foi encontrado. O Job de 5min pode ter falhado.", file=sys.stderr)
        sys.exit(1) 
    try:
       
        df_am = carregar_dados_mare(URL_ARQUIVO_MARE_AM)
        df_chuva_raw = pd.read_csv(nome_arquivo_chuva, sep=CSV_DELIMITADOR)
        df_chuva_raw.rename(columns={'nome': 'nomeEstacao', 'valor': 'valorMedida'}, inplace=True)
        df_chuva_raw['datahora'] = pd.to_datetime(df_chuva_raw['datahora']) 
        
        
        df_vp_calculado = processar_dados_chuva_simplificado(df_chuva_raw, [data_para_processar_str], ESTACOES_DESEJADAS)
        df_risco_final = executar_analise_risco_completa(df_vp_calculado, df_am)
        
        if df_risco_final.empty:
            print("⚠️ Aviso: Cálculo de risco do dia resultou em DataFrame vazio.", file=sys.stderr)
            sys.exit(0)

       
        colunas_saida = ['data', 'hora_ref', 'nomeEstacao', 'VP', 'AM', 'Nivel_Risco_Valor', 'Classificacao_Risco']
        
       
        df_novo_dia = df_risco_final[colunas_saida].copy() 
        
       
        df_novo_dia['data'] = data_para_processar_str
        

    
        df_historico_existente = pd.DataFrame(columns=colunas_saida)
        
        try:
            response = requests.get(URL_ARQUIVO_HISTORICO)
            response.raise_for_status() 
            from io import StringIO
            df_historico_existente = pd.read_csv(StringIO(response.text))
            df_historico_existente = df_historico_existente[colunas_saida] 
            print(f"✅ Histórico existente baixado com {len(df_historico_existente)} linhas.")
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 404:
                print("⚠️ Aviso: Arquivo de histórico (resultado_risco_final.csv) não encontrado no destino. Será criado um novo.")
            else:
                print(f"❌ ERRO HTTP ao baixar histórico: {http_err}", file=sys.stderr)
        except Exception as e:
            print(f"❌ ERRO fatal ao processar o histórico: {e}", file=sys.stderr)
            sys.exit(1)

      
        df_historico_final = pd.concat([df_historico_existente, df_novo_dia], ignore_index=True)
        
        df_historico_final.drop_duplicates(subset=['data', 'hora_ref', 'nomeEstacao'], keep='last', inplace=True)
        
       
        df_historico_final.to_csv(NOME_ARQUIVO_SAIDA_FINAL, index=False)
        print(f"✅ Sucesso! Arquivo de histórico '{NOME_ARQUIVO_SAIDA_FINAL}' (com {len(df_historico_final)} linhas) gerado para envio.")

    except Exception as e:
        print(f"❌ ERRO fatal durante o cálculo de risco: {e}", file=sys.stderr)
        sys.exit(1)
