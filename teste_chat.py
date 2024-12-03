import pandas as pd
import streamlit as st
import requests
import os
import re
import time

from datetime import datetime

from arquivo import ler_planilhas_e_extrair_cnpjs, localizar_arquivo_excel

st.set_page_config(layout="wide", page_title="Consulta CND - Receita Federal")
start = time.time()
ano_atual = datetime.now().year

# URL e argumentos da requisição
url = 'https://api.infosimples.com/api/v2/consultas/receita-federal/pgfn'
args = {
    "token": "",
    "preferencia_emissao": "nova",  # Inicialmente configurado como nova
    "timeout": 300
}

# Data atual para nomeação de arquivos
data_atual = datetime.now()

# Caminho fixo para salvar os PDFs
pasta_destino = fr"G:\Drives compartilhados\Operacional\12 - CONTROLES\CND\{ano_atual}"
os.makedirs(pasta_destino, exist_ok=True)

# Caminho para a subpasta de planilhas
pasta_planilhas = os.path.join(pasta_destino, "planilhas_controle")
os.makedirs(pasta_planilhas, exist_ok=True)

# Nome do arquivo Excel e extração da lista de CNPJs
arquivo_caminho = r"G:\Drives compartilhados\Operacional\19 - AUTOMAÇAO\RPA\TIME INTERNO AUTOMAÇÃO\PLANILHA AVANTSEC"
#arquivo_excel = "PLANILHA DE CONTROLE - 18.11.2024.xlsx"
arquivo_excel = localizar_arquivo_excel(arquivo_caminho)
lista_cnpjs = ler_planilhas_e_extrair_cnpjs(arquivo_excel, sheet1_header=2, sheet2_header=1)
#lista_cnpjs_teste = lista_cnpjs[:3]  


resultados = []
caminhos_pdfs = []
falhas_download = []  # Lista para armazenar CNPJs com falha no download

# Função para limpar e tratar os dados
def limpar_e_tratar_dados(df: pd.DataFrame) -> pd.DataFrame:
    if 'validade' in df.columns:
        df['validade'] = pd.to_datetime(df['validade'], errors='coerce')
    if 'validade_data' in df.columns:
        df['validade_data'] = pd.to_datetime(df['validade_data'], errors='coerce')
    if 'validade_prorrogada' in df.columns:
        df['validade_prorrogada'] = pd.to_datetime(df['validade_prorrogada'], errors='coerce')
    if 'cnpj' in df.columns:
        df['cod_cnpj'] = df['cnpj'].astype(str).str.replace('.', '').str.replace('/', '').str.replace('-', '')
    df['data_consulta_api'] = data_atual
    if 'certidao_codigo' in df.columns:
        df['cod_certidao'] = df['certidao_codigo'].astype(str).str.replace('.', '')
    return df

# Função para salvar o PDF com tentativas de download
def salvar_pdf(link: str, destino: str, max_retries: int = 3) -> bool:
    for attempt in range(max_retries):
        try:
            # Fazendo o download do PDF com stream
            with requests.get(link, stream=True) as response:
                response.raise_for_status()  # Verifica se o download foi bem-sucedido
                
                # Verifica se o conteúdo retornado é um PDF
                content_type = response.headers.get('Content-Type')
                if content_type != 'application/pdf':
                    raise ValueError(f"O arquivo não é um PDF válido. Tipo recebido: {content_type}")

                # Salva o PDF no disco
                with open(destino, 'wb') as pdf_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        pdf_file.write(chunk)

            print(f"PDF salvo com sucesso em: {destino}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {attempt + 1} falhou ao baixar o PDF: {e}")
        except ValueError as e:
            print(f"Tentativa {attempt + 1} falhou: {e}")
        except Exception as e:
            print(f"Tentativa {attempt + 1} falhou devido a erro desconhecido: {e}")
        
        # Espera antes de tentar novamente
        time.sleep(2)

    print(f"Falha no download após {max_retries} tentativas: {link}")
    return False

# Função que processa cada CNPJ individualmente
def processar_cnpj(cnpj: str):
    cnpj_normalizado = re.sub(r'[^\d]', '', cnpj)  # Normalizar CNPJ
    
    # Criar subpasta para o CNPJ
    subpasta_cnpj = os.path.join(pasta_destino, cnpj_normalizado)
    os.makedirs(subpasta_cnpj, exist_ok=True)

    for preferencia in ["nova", "2via"]:
        args["preferencia_emissao"] = preferencia
        args["cnpj"] = cnpj_normalizado
        
        # Fazendo a requisição
        response = requests.post(url, data=args)
        try:
            response_json = response.json()
        except ValueError:
            print(f"Erro ao decodificar a resposta JSON para CNPJ {cnpj_normalizado}.")
            continue
        finally:
            response.close()

        # Se o código for 200, sucesso
        if response_json['code'] == 200:
            print(f"Retorno com sucesso para CNPJ {cnpj_normalizado}: {response_json['data']}")

            # Construção do DataFrame
            if isinstance(response_json['data'], list):
                df = pd.DataFrame(response_json['data'])
                df['code'] = response_json['code']
                df['code_message'] = response_json['code_message']
            elif isinstance(response_json['data'], dict):
                df = pd.DataFrame([response_json['data']])
            else:
                raise ValueError("Formato de 'data' inesperado, esperado lista ou dicionário.")
            
            # Limpando e tratando os dados
            df = limpar_e_tratar_dados(df)
            #resultados.append(df)

            # Verificando e salvando o PDF
            for item in response_json['data']:
                if 'site_receipt' in item:
                    pdf_url = item['site_receipt']
                    data_formatada = data_atual.strftime('%Y-%m-%d_%H-%M-%S')
                    pdf_nome = f"{cnpj_normalizado}_{preferencia}_{data_formatada}.pdf"
                    pdf_caminho = os.path.join(subpasta_cnpj, pdf_nome)

                    if salvar_pdf(pdf_url, pdf_caminho):
                        df['caminho_pdf'] = pdf_caminho
                        resultados.append(df)  # Armazena apenas o sucesso com PDF salvo
                        break  # Sai do loop da preferência após salvar
            else:
                falhas_download.append(cnpj_normalizado)  # Adiciona CNPJ com falha no download

            break  # Sai do loop das preferências após salvar com sucesso

        elif response_json['code'] in range(600, 799):
            # Se o código for de erro (600-799)
            print(f"Erro para o CNPJ {cnpj_normalizado}: {response_json['code']} - {response_json['code_message']}")

# Processando cada CNPJ
for cnpj in lista_cnpjs:
    processar_cnpj(cnpj)

# Consolidando os resultados em um único DataFrame
if resultados:
    df_final = pd.concat(resultados, ignore_index=True)
    df_final = limpar_e_tratar_dados(df_final)

    # Salvar o DataFrame em um arquivo Excel
    nome_arquivo_excel = os.path.join(pasta_planilhas, f"PLANILHA DE CONTROLE - {data_atual.strftime('%Y-%m-%d_%H-%M-%S')}.xlsx")
    df_final.to_excel(nome_arquivo_excel, index=False)
    print(f"Planilha salva com sucesso em: {nome_arquivo_excel}")

    # Aplicando o filtro de razão social no DataFrame final
    filtro_razao_social = st.sidebar.text_input("Filtro por Razão Social", value="")
    if filtro_razao_social:
        df_filtrado = df_final[df_final['razao_social'].str.contains(filtro_razao_social, case=False, na=False)]
    else:
        df_filtrado = df_final

    st.dataframe(df_filtrado)
else:
    st.warning("Nenhum dado de sucesso encontrado.")

end = time.time()
tempo = end - start
print(f"Tempo de execução: {tempo:.2f} segundos")
