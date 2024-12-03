import psycopg2
from psycopg2 import sql

class serviceTaxAllDB():

    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="master",
            user="postgres",
            password="djgr27041965",
            host="localhost",
            port="5432"
            )

    def creating_DB(self, dataset_compensacao):
        self.conn.autocommit = True 
        cur = self.conn.cursor()
        db_name = dataset_compensacao
        try:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Banco de dados '{db_name}' criado com sucesso!")
        except psycopg2.errors.DuplicateDatabase:
            print(f"O banco de dados '{db_name}' já existe.")
        finally:
            cur.close()

    def creatingTables(self, db, formatoDaTabela:str):
            conn = psycopg2.connect(
                dbname=f"{db}",
                user="postgres",
                password="djgr27041965",
                host="localhost",
                port="5432"
            )
            cur = conn.cursor()
            cur.execute(formatoDaTabela)
            conn.commit()
            cur.close()
            conn.close()

if __name__ == '__main__':
    service = serviceTaxAllDB()
    service.creating_DB('db_consultacnd')
    create_table_query_df_consultacnd = '''
        CREATE TABLE IF NOT EXISTS df_consultacnd (
            NOME_CERTIDAO VARCHAR(2000),
            COD_CERTIDAO VARCHAR(100),
            COD_CNPJ VARCHAR(100), 
            COD_CNPJ_STATUS VARCHAR(100),
            TIPO_COMPROVANTE VARCHAR(100),
            STATUS_EMISSAO_CERTIDAO_NEGATIVA VARCHAR(100),
            COD_COMPROVANTE VARCHAR(100),
            DATA_CONSULTA DATE NOT NULL,
            STATUS_DEBITOS_PGFN VARCHAR(100),
            STATUS_DEBITOS_RFB VARCHAR(100),
            DATA_EMISSAO DATE NOT NULL,
            MENSAGEM VARCHAR(2000),
            NOME_CLIENTE VARCHAR(1000),
            COD_CNPJ_NORMALIZADO VARCHAR(100),
            DATA_EMISSAO_COMPLETA DATE NOT NULL,
            RAZAO_SOCIAL VARCHAR(1000),
            STATUS_CERTIDAO VARCHAR(1000),
            TIPO_CERTIDAO VARCHAR(1000),
            DATA_VALIDADE DATE NOT NULL,
            DATA_VALIDADE_PRORROGADA DATE NOT NULL,
            SITE_RESPOSTA VARCHAR(2000),
            COD_RESPOSTA NUMERIC,
            MENSAGEM_RESPOSTA VARCHAR(2000),
            DATA_CONSULTA_API DATE NOT NULL,
            COD_CERTIDAO_NORMALIZADO VARCHAR(100),
            CAMINHO_DRIVE_PDF VARCHAR(2000)
            )
    '''
    service.creatingTables('db_consultacnd', create_table_query_df_consultacnd)