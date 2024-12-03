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


def ler_planilhas_e_extrair_cnpjs(arquivo_excel, sheet1_header, sheet2_header):
    """
    Lê as planilhas de um arquivo Excel, ajusta os cabeçalhos e extrai os CNPJs.
    
    Parâmetros:
        arquivo_excel (str): Nome do arquivo Excel.
        sheet1_header (int): Linha do cabeçalho da primeira planilha (zero-based).
        sheet2_header (int): Linha do cabeçalho da segunda planilha (zero-based).
    
    Retorno:
        lista_cnpjs (list): Lista combinada de CNPJs das duas planilhas.
    """
    try:
        # Leia as planilhas forçando a leitura da coluna 'CNPJ' como texto
        planilha1 = pd.read_excel(arquivo_excel, sheet_name=0, header=sheet1_header, dtype={'CNPJ': str})
        planilha2 = pd.read_excel(arquivo_excel, sheet_name=1, header=sheet2_header, dtype={'CNPJ': str})
        
        # Remova espaços desnecessários nos nomes das colunas
        planilha1.columns = planilha1.columns.str.strip()
        planilha2.columns = planilha2.columns.str.strip()

        # Verifique se a coluna 'CNPJ' está presente
        if 'CNPJ' not in planilha1.columns or 'CNPJ' not in planilha2.columns:
            print("Coluna 'CNPJ' não encontrada em uma das planilhas. Verifique a estrutura do arquivo.")
            return []

        # Extraia os CNPJs da primeira planilha
        cnpj_planilha1 = planilha1['CNPJ'].dropna().apply(lambda x: str(x).split('.')[0]).tolist()

        # Extraia os CNPJs da segunda planilha, filtrando dados inválidos
        cnpj_planilha2 = planilha2['CNPJ'].dropna().apply(lambda x: str(x).split('.')[0]).tolist()

        # Filtrar apenas CNPJs válidos
        cnpj_planilha1 = filtrar_cnpjs_validos(cnpj_planilha1)
        cnpj_planilha2 = filtrar_cnpjs_validos(cnpj_planilha2)

        # Combine os CNPJs de ambas as planilhas
        lista_cnpjs = cnpj_planilha1 + cnpj_planilha2
        return lista_cnpjs

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
lista_cnpjs = ler_planilhas_e_extrair_cnpjs(arquivo_excel, sheet1_header=2, sheet2_header=1)

# Exiba o resultado
# if lista_cnpjs:
#     print("Lista de CNPJs:", lista_cnpjs)
# else:
#     print("Nenhum CNPJ foi encontrado.")
