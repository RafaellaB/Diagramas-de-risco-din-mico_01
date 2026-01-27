# Arquivo: calcular_risco_cli.py
import os
import sys
import requests
import pandas as pd
import numpy as np
import glob
import re
from datetime import datetime
from pytz import timezone
from io import StringIO

URL_ARQUIVO_HISTORICO = 'https://raw.githubusercontent.com/RafaellaB/Painel-Diagrama-de-Risco/main/resultado_risco_final.csv'
URL_ARQUIVO_MARE_AM = 'https://raw.githubusercontent.com/RafaellaB/Diagramas-de-risco-din-mico/main/tide/mare_calculada_hora_em_hora_ano-completo.csv'
NOME_ARQUIVO_SAIDA_FINAL = 'resultado_risco_final.csv'
CSV_DELIMITADOR = ',' 
ESTACOES_DESEJADAS = ["Campina do Barreto", "Torreão", "RECIFE - APAC", "Imbiribeira", "Dois Irmãos"]

def carregar_dados_mare(url_am_data):
    """ Lê a maré corrigindo o erro de coluna 'Hora_Exata' do dia 21. """
    try:
        df_am_raw = pd.read_csv(url_am_data, sep=';', decimal=',')
        df_am_raw.rename(columns={'Hora_Exata': 'datahora', 'Altura_m': 'AM'}, inplace=True)
        
        if 'datahora' not in df_am_raw.columns:
            raise KeyError("Coluna 'Hora_Exata' não encontrada no arquivo de maré.")

        df_am_raw['datahora'] = pd.to_datetime(df_am_raw['datahora'])
        df_am_raw['data'] = df_am_raw['datahora'].dt.strftime('%Y-%m-%d')
        df_am_raw['hora_ref'] = df_am_raw['datahora'].dt.strftime('%H:00:00')
        return df_am_raw[['data', 'hora_ref', 'AM']]
    except Exception as e:
        print(f"ERRO Maré: {e}", file=sys.stderr)
        return pd.DataFrame()

def processar_chuva_arquivo(df_chuva, data_alvo):
    """ Processa os dados de chuva para a data específica do arquivo. """
    df = df_chuva[df_chuva['nomeEstacao'].isin(ESTACOES_DESEJADAS)].copy()
    df['datahora'] = pd.to_datetime(df['datahora'])
    df['data_str'] = df['datahora'].dt.strftime('%Y-%m-%d')
    
    # Filtra apenas os dados pertencentes à data do nome do arquivo
    df = df[df['data_str'] == data_alvo]
    if df.empty: return pd.DataFrame()
    
    df = df.set_index('datahora').sort_index()
    resultados = []
    for estacao, grupo in df.groupby('nomeEstacao'):
        chuva_10min = grupo['valorMedida'].rolling('10min').sum()
        chuva_2h = grupo['valorMedida'].rolling('2h').sum()
        temp = pd.DataFrame({'chuva_10min': chuva_10min, 'chuva_2h': chuva_2h})
        agregado = temp.resample('h').last()
        agregado['VP'] = (agregado['chuva_10min'] * 6) + agregado['chuva_2h']
        agregado['nomeEstacao'] = estacao
        resultados.append(agregado)
    
    df_vp = pd.concat(resultados).reset_index()
    df_vp['data'] = df_vp['datahora'].dt.strftime('%Y-%m-%d')
    df_vp['hora_ref'] = df_vp['datahora'].dt.strftime('%H:00:00')
    return df_vp[['data', 'hora_ref', 'nomeEstacao', 'VP']]

if __name__ == "__main__":
    # 1. Carrega dados de Maré
    df_am = carregar_dados_mare(URL_ARQUIVO_MARE_AM)
    if df_am.empty: sys.exit(1)

    # 2. Varredura automática: encontra todos os arquivos salvos pelo outro script
    arquivos_disponiveis = glob.glob("chuva_recife_*.csv")
    print(f"Encontrados {len(arquivos_disponiveis)} arquivos de chuva no repositório.")

    lista_novos_dados = []

    for arq in arquivos_disponiveis:
        # Extrai a data do nome do arquivo usando Regex
        match = re.search(r'(\d{4}-\d{2}-\d{2})', arq)
        if not match: continue
        data_do_arquivo = match.group(1)
        
        try:
            print(f"Calculando risco para: {data_do_arquivo}...")
            df_raw = pd.read_csv(arq, sep=CSV_DELIMITADOR)
            df_raw.rename(columns={'nome': 'nomeEstacao', 'valor': 'valorMedida'}, inplace=True)
            
            df_vp = processar_chuva_arquivo(df_raw, data_do_arquivo)
            if not df_vp.empty:
                df_mesclado = pd.merge(df_vp, df_am, on=['data', 'hora_ref'], how='left')
                
                # Cálculos finais
                df_mesclado['Nivel_Risco_Valor'] = (df_mesclado['VP'].astype(float) * df_mesclado['AM'].astype(float)).round(2)
                bins = [-np.inf, 30, 50, 100, np.inf]
                labels = ['Baixo', 'Moderado', 'Moderado Alto', 'Alto']
                df_mesclado['Classificacao_Risco'] = pd.cut(df_mesclado['Nivel_Risco_Valor'], bins=bins, labels=labels)
                
                lista_novos_dados.append(df_mesclado)
        except Exception as e:
            print(f"Erro ao processar arquivo {arq}: {e}")

    if not lista_novos_dados:
        print("❌ Nenhum dado novo pôde ser processado.")
        sys.exit(0)

    # 3. Atualização do Histórico Final
    df_total_novo = pd.concat(lista_novos_dados, ignore_index=True)
    
    try:
        res = requests.get(URL_ARQUIVO_HISTORICO)
        df_historico = pd.read_csv(StringIO(res.text)) if res.status_code == 200 else pd.DataFrame()
    except:
        df_historico = pd.DataFrame()

    df_final = pd.concat([df_historico, df_total_novo], ignore_index=True)
    df_final.drop_duplicates(subset=['data', 'hora_ref', 'nomeEstacao'], keep='last', inplace=True)
    df_final.sort_values(['data', 'hora_ref'], ascending=[False, False], inplace=True)
    
    # Salva localmente para que o GitHub Action faça o Push
    df_final.to_csv(NOME_ARQUIVO_SAIDA_FINAL, index=False)
    print(f"✅ Sucesso! {len(df_final)} registros consolidados no histórico.")
