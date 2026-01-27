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
    """ Carrega a maré tratando o novo formato de colunas e separadores. """
    try:
        # Lê com separador ';' e decimal ',' (formato que você mencionou)
        df_am_raw = pd.read_csv(url_am_data, sep=';', decimal=',')
        
        # Correção do mapeamento das colunas
        df_am_raw.rename(columns={'Hora_Exata': 'datahora', 'Altura_m': 'AM'}, inplace=True)
        
        if 'datahora' not in df_am_raw.columns:
            raise KeyError("Coluna 'Hora_Exata' não encontrada no CSV de maré.")

        df_am_raw['datahora'] = pd.to_datetime(df_am_raw['datahora'])
        df_am_raw['data'] = df_am_raw['datahora'].dt.strftime('%Y-%m-%d')
        df_am_raw['hora_ref'] = df_am_raw['datahora'].dt.strftime('%H:00:00')
        return df_am_raw[['data', 'hora_ref', 'AM']]
    except Exception as e:
        print(f"ERRO ao carregar maré: {e}", file=sys.stderr)
        return pd.DataFrame()

def processar_dados_chuva_simplificado(df_chuva, data_alvo, estacoes_desejadas):
    """ Calcula o indicador VP para uma data específica. """
    df = df_chuva[df_chuva['nomeEstacao'].isin(estacoes_desejadas)].copy()
    df['datahora'] = pd.to_datetime(df['datahora'])
    df['data'] = df['datahora'].dt.date.astype(str)
    
    # Filtra apenas a data que estamos processando no momento
    df = df[df['data'] == data_alvo]
    if df.empty: return pd.DataFrame()
    
    df = df.set_index('datahora').sort_index()
    resultados = []
    for estacao, grupo in df.groupby('nomeEstacao'):
        chuva_10min = grupo['valorMedida'].rolling('10min').sum()
        chuva_2h = grupo['valorMedida'].rolling('2h').sum()
        temp_df = pd.DataFrame({'chuva_10min': chuva_10min, 'chuva_2h': chuva_2h})
        agregado = temp_df.resample('h').last()
        agregado['VP'] = (agregado['chuva_10min'] * 6) + agregado['chuva_2h']
        agregado['nomeEstacao'] = estacao
        resultados.append(agregado)
    
    df_vp = pd.concat(resultados).reset_index()
    df_vp['data'] = df_vp['datahora'].dt.strftime('%Y-%m-%d')
    df_vp['hora_ref'] = df_vp['datahora'].dt.strftime('%H:00:00')
    return df_vp[['data', 'hora_ref', 'nomeEstacao', 'VP']]

if __name__ == "__main__":
    # 1. Carrega Maré
    df_am = carregar_dados_mare(URL_ARQUIVO_MARE_AM)
    if df_am.empty: sys.exit(1)

    # 2. Busca TODOS os arquivos de chuva salvos no repositório
    arquivos_chuva = glob.glob("chuva_recife_*.csv")
    print(f"Foram encontrados {len(arquivos_chuva)} arquivos de chuva.")

    lista_novos_resultados = []

    for arquivo in arquivos_chuva:
        # Extrai a data do nome do arquivo (ex: 2026-01-21)
        data_match = re.search(r'(\d{4}-\d{2}-\d{2})', arquivo)
        if not data_match: continue
        data_str = data_match.group(1)
        
        try:
            print(f"Processando arquivo: {arquivo}...")
            df_chuva_raw = pd.read_csv(arquivo, sep=CSV_DELIMITADOR)
            df_chuva_raw.rename(columns={'nome': 'nomeEstacao', 'valor': 'valorMedida'}, inplace=True)
            
            df_vp = processar_dados_chuva_simplificado(df_chuva_raw, data_str, ESTACOES_DESEJADAS)
            
            if not df_vp.empty:
                df_dia = pd.merge(df_vp, df_am, on=['data', 'hora_ref'], how='left')
                
                # Cálculos de Risco
                df_dia['VP'] = pd.to_numeric(df_dia['VP'], errors='coerce').fillna(0)
                df_dia['AM'] = pd.to_numeric(df_dia['AM'], errors='coerce').fillna(0)
                df_dia['Nivel_Risco_Valor'] = (df_dia['VP'] * df_dia['AM']).round(2)
                
                bins = [-np.inf, 30, 50, 100, np.inf]
                labels = ['Baixo', 'Moderado', 'Moderado Alto', 'Alto']
                df_dia['Classificacao_Risco'] = pd.cut(df_dia['Nivel_Risco_Valor'], bins=bins, labels=labels, right=False)
                
                lista_novos_resultados.append(df_dia)
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")

    if not lista_novos_resultados:
        print("Nada novo para processar.")
        sys.exit(0)

    # 3. Consolidação com Histórico
    df_acumulado = pd.concat(lista_novos_resultados, ignore_index=True)
    
    try:
        res = requests.get(URL_ARQUIVO_HISTORICO)
        df_hist = pd.read_csv(StringIO(res.text)) if res.status_code == 200 else pd.DataFrame()
    except:
        df_hist = pd.DataFrame()

    df_final_historico = pd.concat([df_hist, df_acumulado], ignore_index=True)
    df_final_historico.drop_duplicates(subset=['data', 'hora_ref', 'nomeEstacao'], keep='last', inplace=True)
    
    # Ordenação decrescente (mais recente primeiro)
    df_final_historico.sort_values(['data', 'hora_ref'], ascending=[False, False], inplace=True)
    
    df_final_historico.to_csv(NOME_ARQUIVO_SAIDA_FINAL, index=False)
    print(f"✅ Sucesso! Histórico atualizado no arquivo '{NOME_ARQUIVO_SAIDA_FINAL}'.")
