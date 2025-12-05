# -*- coding: utf-8 -*-
import psycopg2
from src.tests.test_db_connection import DBConnection


class DatabaseCleaner:
    def __init__(self, db_connection):
        """
        Inicializa o cleaner com uma conexão de banco de dados

        Args:
            db_connection: Instância da classe DBConnection
        """
        self.db_connection = db_connection
        self.conn = None

    def check_and_clean_database(self):
        """
        Verifica se há dados no banco e limpa todas as tabelas se houver

        Returns:
            bool: True se dados foram limpos, False se não havia dados
        """
        try:
            # Conecta ao banco
            self.conn = self.db_connection.connect()
            if not self.conn:
                print("ERRO: Não foi possível conectar ao banco de dados!")
                return False

            cur = self.conn.cursor()

            # Verifica se há dados nas tabelas bronze, silver e gold
            tabelas_para_verificar = [
                'bronze.fipe_raw',
                'silver.fipe_limited',
                'gold.fipe_summary'
            ]

            dados_encontrados = False

            for tabela in tabelas_para_verificar:
                # Verifica se a tabela existe
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema || '.' || table_name = %s
                    );
                """, (tabela,))

                tabela_existe = cur.fetchone()[0]

                if tabela_existe:
                    # Conta registros na tabela
                    schema, nome_tabela = tabela.split('.')
                    cur.execute(f"SELECT COUNT(*) FROM {schema}.{nome_tabela};")
                    count = cur.fetchone()[0]

                    if count > 0:
                        dados_encontrados = True
                        print(f"✓ Tabela {tabela}: {count} registros encontrados")
                    else:
                        print(f"✓ Tabela {tabela}: vazia")
                else:
                    print(f"⚠ Tabela {tabela}: não existe ainda")

            # Se encontrou dados, limpa todas as tabelas
            if dados_encontrados:
                print("\nDados encontrados no banco. Limpando todas as tabelas...")
                self._limpar_todas_tabelas(cur)
                print("Banco de dados limpo com sucesso!")
                return True
            else:
                print("\nBanco de dados está vazio. Pronto para inserir novos dados.")
                return False

        except Exception as e:
            print(f"ERRO ao verificar/limpar banco: {e}")
            return False
        finally:
            if self.conn:
                self.conn.commit()
                self.conn.close()

    def _limpar_todas_tabelas(self, cursor):
        """
        Limpa todas as tabelas do banco de dados

        Args:
            cursor: Cursor do banco de dados
        """
        # Ordem correta para limpeza (devido a foreign keys se houver)
        tabelas_para_limpar = [
            'gold.fipe_summary',
            'silver.fipe_limited',
            'bronze.fipe_raw'
        ]

        for tabela in tabelas_para_limpar:
            try:
                # Verifica se a tabela existe antes de tentar limpar
                schema, nome_tabela = tabela.split('.')
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    );
                """, (schema, nome_tabela))

                if cursor.fetchone()[0]:
                    cursor.execute(f"DELETE FROM {tabela};")
                    print(f"  - Tabela {tabela}: limpa")
                else:
                    print(f"  - Tabela {tabela}: não existe (pulando)")

            except Exception as e:
                print(f"  ⚠ Aviso ao limpar {tabela}: {e}")

    def clean_all_tables_force(self):
        """
        Limpa todas as tabelas sem verificar se há dados (forçado)

        Returns:
            bool: True se sucesso, False se erro
        """
        try:
            self.conn = self.db_connection.connect()
            if not self.conn:
                return False

            cur = self.conn.cursor()
            self._limpar_todas_tabelas(cur)
            self.conn.commit()
            print("Todas as tabelas foram limpas (forçado)")
            return True

        except Exception as e:
            print(f"ERRO ao limpar tabelas: {e}")
            return False
        finally:
            if self.conn:
                self.conn.close()


if __name__ == "__main__":
    # Teste da classe
    print("=== TESTE DatabaseCleaner ===\n")

    # Criar conexão
    db = DBConnection("localhost", 5432, "api_fipe", "postgres", "postgres")

    # Criar cleaner
    cleaner = DatabaseCleaner(db)

    # Testar verificação e limpeza
    cleaner.check_and_clean_database()