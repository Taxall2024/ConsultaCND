import pandas as pd
import os
import datetime


def localizar_arquivo_excel(base_path: str) -> str:
    """
    Localiza o arquivo Excel em uma estrutura de pastas organizada por ano/mês.
    Lida com subpastas nos formatos 'DD.MM.AAAA' e 'DD MM AAAA'.

    Parâmetros:
        base_path (str): Caminho base onde estão as pastas de arquivos Excel.

    Retorna:
        str: Caminho completo do arquivo Excel encontrado.

    Lança:
        FileNotFoundError: Se o arquivo ou as pastas esperadas não forem encontradas.
    """
    # Obter o ano e mês corrente
    ano_atual = datetime.datetime.now().year
    mes_atual = datetime.datetime.now().month
    mes_atual_str = f"{mes_atual:02d}"  # Para garantir que o mês tenha 2 dígitos

    # Caminho da pasta do ano corrente
    caminho_ano = os.path.join(base_path, str(ano_atual), f"{ano_atual}-{mes_atual_str}")
    print(f"Procurando na pasta do ano/mês: {caminho_ano}")  # Log para depuração

    # Verificar se a pasta do mês existe
    if not os.path.exists(caminho_ano):
        raise FileNotFoundError(f"A pasta para o ano e mês corrente não foi encontrada: {caminho_ano}")

    # Procurar pelas subpastas dentro dessa pasta
    for subpasta in os.listdir(caminho_ano):
        subpasta_path = os.path.join(caminho_ano, subpasta)
        print(f"Verificando subpasta: {subpasta_path}")  # Log para depuração

        # Verificar se a subpasta corresponde a um dos formatos esperados de data
        data_subpasta = None
        for formato_data in ["%d.%m.%Y", "%d %m %Y"]:
            try:
                data_subpasta = datetime.datetime.strptime(subpasta, formato_data)
                break  # Se o formato é válido, sair do loop
            except ValueError:
                continue  # Tentar o próximo formato

        # Se a subpasta não corresponder a nenhum formato de data, ignorá-la
        if data_subpasta is None:
            print(f"Subpasta ignorada (formato inválido): {subpasta}")
            continue

        # Verifica se é uma segunda-feira
        if data_subpasta.weekday() == 0:  # Segunda-feira
            # Buscar arquivos que contenham "PLANILHA DE CONTROLE"
            for arquivo in os.listdir(subpasta_path):
                if "PLANILHA DE CONTROLE" in arquivo and arquivo.endswith(".xlsx"):
                    arquivo_excel_path = os.path.join(subpasta_path, arquivo)
                    print(f"Arquivo Excel encontrado: {arquivo_excel_path}")
                    return arquivo_excel_path

            print(f"Arquivo Excel esperado, mas não encontrado em: {subpasta_path}")

    # Caso o arquivo não seja encontrado
    raise FileNotFoundError(f"Não foi encontrado o arquivo Excel no caminho esperado: {caminho_ano}")


def filtrar_cnpjs_validos(cnpjs):
    """
    Filtra os CNPJs para garantir que sejam válidos (apenas números, com 14 caracteres).
    
    Parâmetros:
        cnpjs (list): Lista de CNPJs em formato de string.
    
    Retorno:
        list: Lista com CNPJs válidos.
    """
    return [cnpj for cnpj in cnpjs if len(cnpj) == 14 and cnpj.isdigit()]


def ler_planilhas_e_extrair_cnpjs(arquivo_excel: str, sheet1_header: int = 2, sheet2_header: int = 1) -> pd.DataFrame:
    """
    Processa as planilhas de um arquivo Excel para extrair as colunas 'CNPJ' e 'Empresa'.
    
    Args:
        arquivo_excel (str): Caminho do arquivo Excel a ser processado.
        sheet1_header (int): Número da linha que contém o cabeçalho da primeira aba.
        sheet2_header (int): Número da linha que contém o cabeçalho da segunda aba.
    
    Returns:
        pd.DataFrame: DataFrame consolidado e limpo contendo as colunas 'CNPJ' e 'Empresa'.
    """
    try:
        # Ler as abas do Excel
        df_aba1 = pd.read_excel(arquivo_excel, sheet_name=0, header=sheet1_header)  # Primeira aba
        df_aba2 = pd.read_excel(arquivo_excel, sheet_name=1, header=sheet2_header)  # Segunda aba

        # Verificar e ajustar as colunas
        colunas_aba1 = df_aba1.columns.str.strip()
        colunas_aba2 = df_aba2.columns.str.strip()

        if 'CNPJ' not in colunas_aba1 or ('Empresa' not in colunas_aba1 and 'EMPRESA' not in colunas_aba1):
            raise KeyError("Colunas 'CNPJ' ou 'Empresa'/'EMPRESA' não encontradas na primeira aba.")
        if 'CNPJ' not in colunas_aba2 or ('Empresa' not in colunas_aba2 and 'EMPRESA' not in colunas_aba2):
            raise KeyError("Colunas 'CNPJ' ou 'Empresa'/'EMPRESA' não encontradas na segunda aba.")
        
        # Selecionar as colunas relevantes
        df_aba1 = df_aba1[['CNPJ', 'Empresa' if 'Empresa' in colunas_aba1 else 'EMPRESA']]
        df_aba2 = df_aba2[['CNPJ', 'Empresa' if 'Empresa' in colunas_aba2 else 'EMPRESA']]

        # Limpar os valores de CNPJ e garantir que sejam strings de 14 dígitos
        df_aba1['CNPJ'] = df_aba1['CNPJ'].dropna().apply(lambda x: str(x).split('.')[0].zfill(14))
        df_aba2['CNPJ'] = df_aba2['CNPJ'].dropna().apply(lambda x: str(x).split('.')[0].zfill(14))

        # Remover linhas com valores ausentes
        df_aba1 = df_aba1.dropna(subset=['CNPJ', 'EMPRESA']).reset_index(drop=True)
        df_aba2 = df_aba2.dropna(subset=['CNPJ', 'EMPRESA']).reset_index(drop=True)

        # Concatenar as duas abas
        df_empresas = pd.concat([df_aba1, df_aba2], ignore_index=True)

        # Limpar e padronizar os nomes das colunas
        df_empresas.columns = df_empresas.columns.str.strip()

        return df_empresas

    except Exception as e:
        print(f"Erro ao processar o arquivo Excel: {e}")
        return []


# Chamada da função
arquivo_caminho = r"G:\Drives compartilhados\Operacional\19 - AUTOMAÇAO\RPA\TIME INTERNO AUTOMAÇÃO\PLANILHA AVANTSEC"

# Localizar o arquivo Excel
try:
    arquivo_excel = localizar_arquivo_excel(arquivo_caminho)
except FileNotFoundError as e:
    print(f"Erro: {e}")
    exit()

# Ler e extrair CNPJs
lista_cnpjs = ler_planilhas_e_extrair_cnpjs(arquivo_excel)
print(lista_cnpjs)
# Exiba o resultado
# if lista_cnpjs:
#     print("Lista de CNPJs:", lista_cnpjs)
# else:
#     print("Nenhum CNPJ foi encontrado.")
