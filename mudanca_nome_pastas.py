import os
import pandas as pd
import re
import streamlit as st
from datetime import datetime
from arquivo import ler_planilhas_e_extrair_cnpjs, localizar_arquivo_excel

# Caminho onde estão as pastas dos CNPJs
ano_atual = datetime.now().year
#pasta_destino = fr"G:\Drives compartilhados\Operacional\12 - CONTROLES\CND\{ano_atual}"
pasta_destino = fr'G:\Drives compartilhados\Operacional\12 - CONTROLES\CND\2024'

# Carregar as duas abas da planilha (ajuste o nome das abas conforme necessário)
arquivo_caminho = r"G:\Drives compartilhados\Operacional\19 - AUTOMAÇAO\RPA\TIME INTERNO AUTOMAÇÃO\PLANILHA AVANTSEC"
arquivo_excel = localizar_arquivo_excel(arquivo_caminho)
df_aba1 = pd.read_excel(arquivo_excel, sheet_name=0, header=2)  # Primeira aba
df_aba2 = pd.read_excel(arquivo_excel, sheet_name=1, header=1)  # Segunda aba

# Verificar e ajustar as colunas 'CNPJ' e 'Empresa' / 'EMPRESA'
colunas_aba1 = df_aba1.columns.str.strip()
colunas_aba2 = df_aba2.columns.str.strip()

# Verifique se as colunas 'CNPJ' e 'Empresa' ou 'EMPRESA' existem nas duas abas
if 'CNPJ' not in colunas_aba1 or ('Empresa' not in colunas_aba1 and 'EMPRESA' not in colunas_aba1):
    raise KeyError("Colunas 'CNPJ' ou 'Empresa'/'EMPRESA' não encontradas na primeira aba.")
if 'CNPJ' not in colunas_aba2 or ('Empresa' not in colunas_aba2 and 'EMPRESA' not in colunas_aba2):
    raise KeyError("Colunas 'CNPJ' ou 'Empresa'/'EMPRESA' não encontradas na segunda aba.")

# Selecionar apenas as colunas 'CNPJ' e a coluna de empresa correspondente
df_aba1 = df_aba1[['CNPJ', 'Empresa' if 'Empresa' in colunas_aba1 else 'EMPRESA']]  # Ajuste conforme o nome exato
df_aba2 = df_aba2[['CNPJ', 'Empresa' if 'Empresa' in colunas_aba2 else 'EMPRESA']]  # Ajuste conforme o nome exato

# Limpar os valores de CNPJ e garantir que sejam strings, removendo qualquer '.0'
df_aba1['CNPJ'] = df_aba1['CNPJ'].dropna().apply(lambda x: str(x).split('.')[0].zfill(14))  # Garantir que tenha 14 dígitos
df_aba2['CNPJ'] = df_aba2['CNPJ'].dropna().apply(lambda x: str(x).split('.')[0].zfill(14))  # Garantir que tenha 14 dígitos

df_aba1 = df_aba1.dropna(subset=['CNPJ', 'EMPRESA']).reset_index(drop=True)
df_aba2 = df_aba2.dropna(subset=['CNPJ', 'EMPRESA']).reset_index(drop=True)

# Concatenar as duas planilhas
df_empresas = pd.concat([df_aba1, df_aba2], ignore_index=True)
df_empresas.columns = df_empresas.columns.str.strip().dropna().tolist()

# Mostrar o dataframe no Streamlit
st.dataframe(df_empresas)

# Função para renomear a pasta
def renomear_pasta(cnpj_pasta, nome_empresa):
    # Normalizar o nome da empresa, removendo caracteres inválidos
    nome_empresa_normalizado = re.sub(r'[<>:"/\\|?*]', '', nome_empresa).strip()  # Remove espaços extras no final
    
    # Verificar se o nome da pasta é muito longo (Windows tem um limite de 260 caracteres)
    caminho_pasta_base = os.path.join(pasta_destino, nome_empresa_normalizado)
    if len(caminho_pasta_base) > 255:  # Aproxima-se do limite do Windows
        nome_empresa_normalizado = nome_empresa_normalizado[:255]  # Limitar o nome da pasta a 255 caracteres
    
    # Verificar se a pasta com o nome da empresa já existe
    nova_pasta_path = os.path.join(pasta_destino, nome_empresa_normalizado)
    
    if os.path.exists(nova_pasta_path):
        # Se a pasta já existir, gerar um novo nome único
        i = 1
        while os.path.exists(f"{nova_pasta_path}_{i}"):
            i += 1
        nova_pasta_path = f"{nova_pasta_path}_{i}"
    
    # Renomear a pasta
    os.rename(cnpj_pasta, nova_pasta_path)
    return nova_pasta_path

# Iterar sobre as pastas existentes
for pasta in os.listdir(pasta_destino):
    pasta_path = os.path.join(pasta_destino, pasta)
    
    # Verificar se a pasta é um diretório e contém um CNPJ no nome
    if os.path.isdir(pasta_path):
        cnpj_pasta = re.sub(r'[^\d]', '', pasta)  # Normaliza o CNPJ do nome da pasta

        # Verificar se o CNPJ da pasta existe no DataFrame
        empresa = df_empresas[df_empresas['CNPJ'] == cnpj_pasta]['EMPRESA'].values

        if empresa.size > 0:
            nome_empresa = empresa[0]  # Pega o nome da empresa correspondente
            
            # Renomear a pasta
            try:
                nova_pasta_path = renomear_pasta(pasta_path, nome_empresa)
                st.write(f"Pasta {pasta} renomeada para {os.path.basename(nova_pasta_path)}")
            except OSError as e:
                st.write(f"Erro ao renomear a pasta {pasta}: {str(e)}")
        else:
            st.write(f"CNPJ {cnpj_pasta} não encontrado no DataFrame. A pasta {pasta} não será renomeada.")
