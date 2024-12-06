import pandas as pd
import streamlit as st
import psycopg2
from psycopg2 import sql

from sqlalchemy import create_engine

class dbController():

    def __init__(self, url):
        self.url = url
        try:
            self.engine = create_engine(self.url)
            self.conn = self.engine.connect()
            self.transaction = self.conn.begin()
            print("Conexão com o banco de dados realizada com sucesso.")
            
        except Exception as e:
            print(f"Erro na conexão com o banco de dados: {e}")
            self.conn = None

    def listar_tabelas(self):
        if self.conn:
            try:
                query_tabelas = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                """

                nomes_tabelas = pd.read_sql(query_tabelas, self.conn)
                return nomes_tabelas['table_name'].tolist()
            
            except Exception as e:
                print(f"Erro ao listar as tabelas: {e}")
                return None
        else:
            print("Não foi possível listar as tabelas porque a conexão não está ativa.")
            return None
        
    def ler_tabela(self, nome_tabela):
        if self.conn:
            try:
                query = f"SELECT * FROM {nome_tabela}"
                df = pd.read_sql(query, self.conn)
                print(f"Dados da tabela {nome_tabela} lidos com sucesso.")
                return df

            except Exception as e:
                print(f"Erro ao ler a tabela {nome_tabela}: {e}")
                return None

    def inserir_dados(self, nome_tabela, metodo = 'append'):
       if self.conn:
            try:
                cur = self.conn.cursor()
        
        # Mapeando as colunas do DataFrame para as colunas da tabela
                for index, row in nome_tabela.iterrows():
                    insert_query = sql.SQL("""
                        INSERT INTO {} (
                            NOME_CERTIDAO, COD_CERTIDAO, COD_CNPJ, COD_CNPJ_STATUS, TIPO_COMPROVANTE,
                            STATUS_EMISSAO_CERTIDAO_NEGATIVA, COD_COMPROVANTE, DATA_CONSULTA, STATUS_DEBITOS_PGFN,
                            STATUS_DEBITOS_RFB, DATA_EMISSAO, MENSAGEM, NOME_CLIENTE, COD_CNPJ_NORMALIZADO,
                            DATA_EMISSAO_COMPLETA, RAZAO_SOCIAL, STATUS_CERTIDAO, TIPO_CERTIDAO, DATA_VALIDADE,
                            DATA_VALIDADE_PRORROGADA, SITE_RESPOSTA, COD_RESPOSTA, MENSAGEM_RESPOSTA, DATA_CONSULTA_API,
                            COD_CERTIDAO_NORMALIZADO, CAMINHO_DRIVE_PDF
                        ) VALUES (
                            %(NOME_CERTIDAO)s, %(COD_CERTIDAO)s, %(COD_CNPJ)s, %(COD_CNPJ_STATUS)s, %(TIPO_COMPROVANTE)s,
                            %(STATUS_EMISSAO_CERTIDAO_NEGATIVA)s, %(COD_COMPROVANTE)s, %(DATA_CONSULTA)s, %(STATUS_DEBITOS_PGFN)s,
                            %(STATUS_DEBITOS_RFB)s, %(DATA_EMISSAO)s, %(MENSAGEM)s, %(NOME_CLIENTE)s, %(COD_CNPJ_NORMALIZADO)s,
                            %(DATA_EMISSAO_COMPLETA)s, %(RAZAO_SOCIAL)s, %(STATUS_CERTIDAO)s, %(TIPO_CERTIDAO)s, %(DATA_VALIDADE)s,
                            %(DATA_VALIDADE_PRORROGADA)s, %(SITE_RESPOSTA)s, %(COD_RESPOSTA)s, %(MENSAGEM_RESPOSTA)s, %(DATA_CONSULTA_API)s,
                            %(COD_CERTIDAO_NORMALIZADO)s, %(CAMINHO_DRIVE_PDF)s
                        );
                    """).format(sql.Identifier('df_consultacnd'))
                    
                    # Preenchendo os valores com base nas colunas do DataFrame
                    data_dict = row.to_dict()
                    cur.execute(insert_query, data_dict)
                
                # Commit das inserções no banco
                self.conn.commit()
                cur.close()
                print("Dados inseridos com sucesso no banco de dados.")
                
            except Exception as e:
                st.error(f"Erro ao inserir os dados na tabela {nome_tabela}: {e}")
    

    def fechar_conexao(self):
       if self.conn:
        self.conn.close()
        print("Conexão com o banco de dados encerrada.")


url = 'postgresql+psycopg2://postgres:djgr27041965@localhost:5432/db_consultacnd'

try:
    banco = dbController(url)
    tabelas = banco.listar_tabelas()
    print(f"Tabelas encontradas: {tabelas}")

except Exception as e:
    print(f"Erro: {e}")

