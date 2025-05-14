# Lotofacil_Analysis/src/database_manager.py
import sqlite3
import pandas as pd
import logging
from typing import List, Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        try:
            self.connect()
            # _create_all_tables deve ser chamado aqui se você quiser que as tabelas
            # sejam criadas/verificadas na instanciação.
            # Vou comentar por enquanto, pois o main.py pode estar chamando-o
            # ou ele pode ser chamado seletivamente.
            # self._create_all_tables() # Descomente se necessário na inicialização
            logger.info(f"Database Manager inicializado e conectado a: {db_path}")
        except sqlite3.Error as e:
            logger.error(f"Erro ao inicializar o DatabaseManager para {db_path}: {e}", exc_info=True)
            raise

    def connect(self) -> None:
        """Estabelece a conexão com o banco de dados SQLite."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON;")
            self.cursor = self.conn.cursor()
            logger.debug(f"Conexão com o banco de dados {self.db_path} estabelecida.")
        except sqlite3.Error as e:
            logger.error(f"Erro ao conectar ao banco de dados {self.db_path}: {e}", exc_info=True)
            raise

    def close(self) -> None:
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            try:
                self.conn.close()
                logger.debug(f"Conexão com o banco de dados {self.db_path} fechada.")
            except sqlite3.Error as e:
                logger.error(f"Erro ao fechar a conexão com o banco de dados: {e}", exc_info=True)
        self.conn = None
        self.cursor = None

    def _execute_query(self, query: str, params: Tuple = None, commit: bool = False) -> None:
        if not self.conn or not self.cursor:
            logger.error("Tentativa de executar query sem uma conexão ativa.")
            raise sqlite3.Error("Conexão com o banco de dados não está ativa.")
        try:
            logger.debug(f"Executando query: {query} com params: {params}")
            self.cursor.execute(query, params or ())
            if commit:
                self.conn.commit()
                logger.debug("Query comitada.")
        except sqlite3.Error as e:
            logger.error(f"Erro ao executar query: {query} - {e}", exc_info=True)
            raise

    def save_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> None:
        if not self.conn:
            logger.error("Tentativa de salvar DataFrame sem uma conexão ativa.")
            raise sqlite3.Error("Conexão com o banco de dados não está ativa.")
        if df is None or df.empty:
            logger.warning(f"DataFrame para a tabela '{table_name}' está vazio ou é None. Nada será salvo.")
            return
        try:
            logger.info(f"Salvando DataFrame na tabela '{table_name}' (if_exists='{if_exists}'). Linhas: {len(df)}")
            df.to_sql(table_name, self.conn, if_exists=if_exists, index=False)
            logger.info(f"DataFrame salvo com sucesso na tabela '{table_name}'.")
        except Exception as e:
            logger.error(f"Erro ao salvar DataFrame na tabela '{table_name}': {e}", exc_info=True)
            raise

    def load_dataframe(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Carrega uma tabela inteira do banco de dados para um DataFrame pandas.
        """
        if not self.table_exists(table_name):
            logger.warning(f"Tabela '{table_name}' não existe. Retornando DataFrame vazio.")
            return pd.DataFrame()
        if not self.conn:
            logger.error(f"Tentativa de carregar tabela '{table_name}' sem conexão.")
            return pd.DataFrame()
        try:
            logger.info(f"Carregando DataFrame da tabela '{table_name}'.")
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn)
            logger.info(f"DataFrame da tabela '{table_name}' carregado com {len(df)} linhas.")
            return df
        except Exception as e:
            logger.error(f"Erro ao carregar DataFrame da tabela '{table_name}': {e}", exc_info=True)
            return pd.DataFrame()

    # Alias para compatibilidade com o código existente que usa load_dataframe_from_db
    def load_dataframe_from_db(self, table_name: str) -> Optional[pd.DataFrame]:
        logger.debug(f"Chamando load_dataframe_from_db, redirecionando para load_dataframe para tabela: {table_name}")
        return self.load_dataframe(table_name)

    def table_exists(self, table_name: str) -> bool:
        if not self.cursor:
            logger.debug("Tentativa de verificar tabela sem um cursor ativo, retornando False.")
            return False
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Erro ao verificar a existência da tabela '{table_name}': {e}", exc_info=True)
            return False
        
    # NOVO MÉTODO ADICIONADO
    def get_table_names(self) -> List[str]:
        """Retorna uma lista com os nomes de todas as tabelas no banco de dados."""
        if self.cursor is None:
            logger.error("Não é possível obter nomes de tabelas: Cursor não inicializado.")
            return []
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            tables = self.cursor.fetchall()
            return [table[0] for table in tables]
        except sqlite3.Error as e:
            logger.error(f"Erro ao buscar nomes de tabelas: {e}", exc_info=True)
            return []
    
    def _create_all_tables(self) -> None:
        """Cria todas as tabelas necessárias para a aplicação, se não existirem."""
        logger.info("Verificando e criando todas as tabelas do banco de dados se não existirem...")
        
        # Essenciais que vi nos logs ou são comuns
        self._create_table_frequencia_absoluta()
        self._create_table_frequencia_relativa()
        self._create_table_atraso_atual()
        self._create_table_atraso_maximo()
        self._create_table_atraso_maximo_separado() # Para o step específico
        self._create_table_atraso_medio()
        self._create_table_pair_metrics() # Tabela para análise de pares
        self._create_frequent_itemsets_table()

        # Tabelas de Ciclo
        self._create_table_ciclos_detalhe()
        self._create_table_ciclos_sumario_estatisticas()
        self._create_table_ciclo_progression()
        self._create_table_ciclo_metric_frequency()
        self._create_table_ciclo_metric_atraso_medio()
        self._create_table_ciclo_metric_atraso_maximo()
        self._create_table_ciclo_metric_atraso_final()
        self._create_table_ciclo_rank_frequency()
        self._create_table_ciclo_group_metrics()

        # Outras tabelas
        self._create_table_propriedades_numericas_por_concurso()
        self._create_table_analise_repeticao_concurso_anterior()
        
        # Tabelas de Chunk (exemplos, você precisará de todas)
        # self._create_table_evol_metric_frequency_linear_50() # Exemplo
        # self._create_table_evol_block_group_metrics_linear_50() # Exemplo
        # self._create_table_bloco_analises_consolidadas_linear_50() # Exemplo

        # Tabelas de Rank Trend
        # self._create_table_evol_rank_frequency_bloco_linear_50() # Exemplo
        self._create_table_rank_geral_dezenas_por_frequencia()
        
        logger.info("Verificação e criação de tabelas (essenciais listadas) concluída.")

    # ----- Definições de Tabelas Específicas -----
    def _create_frequent_itemsets_table(self) -> None:
        query = "CREATE TABLE IF NOT EXISTS frequent_itemsets (itemset_str TEXT PRIMARY KEY, support REAL NOT NULL, length INTEGER NOT NULL, frequency_count INTEGER NOT NULL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'frequent_itemsets' OK.")

    def _create_table_frequencia_absoluta(self):
        query = "CREATE TABLE IF NOT EXISTS frequencia_absoluta (\"Dezena\" INTEGER PRIMARY KEY, \"Frequencia Absoluta\" INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'frequencia_absoluta' OK.")

    def _create_table_frequencia_relativa(self):
        query = "CREATE TABLE IF NOT EXISTS frequencia_relativa (\"Dezena\" INTEGER PRIMARY KEY, \"Frequencia Relativa\" REAL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'frequencia_relativa' OK.")

    def _create_table_atraso_atual(self):
        query = "CREATE TABLE IF NOT EXISTS atraso_atual (\"Dezena\" INTEGER PRIMARY KEY, \"Atraso Atual\" INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'atraso_atual' OK.")

    def _create_table_atraso_maximo(self):
        query = "CREATE TABLE IF NOT EXISTS atraso_maximo (\"Dezena\" INTEGER PRIMARY KEY, \"Atraso Maximo\" INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'atraso_maximo' OK.")
    
    def _create_table_atraso_maximo_separado(self): # Tabela para o step separado
        query = "CREATE TABLE IF NOT EXISTS atraso_maximo_separado (\"Dezena\" INTEGER PRIMARY KEY, \"Atraso Maximo\" INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'atraso_maximo_separado' OK.")

    def _create_table_atraso_medio(self):
        query = "CREATE TABLE IF NOT EXISTS atraso_medio (\"Dezena\" INTEGER PRIMARY KEY, \"Atraso Medio\" REAL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'atraso_medio' OK.")
        
    def _create_table_pair_metrics(self):
        query = "CREATE TABLE IF NOT EXISTS pair_metrics (pair_str TEXT PRIMARY KEY, frequency INTEGER, last_contest INTEGER, current_delay INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'pair_metrics' OK.")

    def _create_table_ciclos_detalhe(self):
        query = "CREATE TABLE IF NOT EXISTS ciclos_detalhe (ciclo_num INTEGER, concurso_inicio INTEGER, concurso_fim INTEGER, duracao_concursos INTEGER, numeros_faltantes TEXT, qtd_faltantes INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclos_detalhe' OK.")

    def _create_table_ciclos_sumario_estatisticas(self):
        query = "CREATE TABLE IF NOT EXISTS ciclos_sumario_estatisticas (total_ciclos_fechados INTEGER, duracao_media_ciclo REAL, duracao_min_ciclo INTEGER, duracao_max_ciclo INTEGER, duracao_mediana_ciclo REAL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclos_sumario_estatisticas' OK.")
        
    def _create_table_ciclo_progression(self):
        query = """CREATE TABLE IF NOT EXISTS ciclo_progression (
                    Concurso INTEGER, Data TEXT, ciclo_num_associado INTEGER, 
                    dezenas_sorteadas_neste_concurso TEXT, 
                    numeros_que_faltavam_antes_deste_concurso TEXT, 
                    qtd_faltavam_antes_deste_concurso INTEGER, 
                    dezenas_apuradas_neste_concurso TEXT, 
                    qtd_apuradas_neste_concurso INTEGER, 
                    numeros_faltantes_apos_este_concurso TEXT, 
                    qtd_faltantes_apos_este_concurso INTEGER, 
                    ciclo_fechou_neste_concurso INTEGER
                 );"""
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_progression' OK.")

    def _create_table_ciclo_metric_frequency(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_metric_frequency (ciclo_num INTEGER, dezena INTEGER, frequencia_no_ciclo INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_metric_frequency' OK.")
    
    def _create_table_ciclo_metric_atraso_medio(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_metric_atraso_medio (ciclo_num INTEGER, dezena INTEGER, atraso_medio_no_ciclo REAL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_metric_atraso_medio' OK.")

    def _create_table_ciclo_metric_atraso_maximo(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_metric_atraso_maximo (ciclo_num INTEGER, dezena INTEGER, atraso_maximo_no_ciclo INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_metric_atraso_maximo' OK.")

    def _create_table_ciclo_metric_atraso_final(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_metric_atraso_final (ciclo_num INTEGER, dezena INTEGER, atraso_final_no_ciclo INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_metric_atraso_final' OK.")

    def _create_table_ciclo_rank_frequency(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_rank_frequency (ciclo_num INTEGER, dezena INTEGER, frequencia_no_ciclo INTEGER, rank_freq_no_ciclo INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_rank_frequency' OK.")

    def _create_table_ciclo_group_metrics(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_group_metrics (ciclo_num INTEGER, avg_pares_no_ciclo REAL, avg_impares_no_ciclo REAL, avg_primos_no_ciclo REAL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_group_metrics' OK.")

    def _create_table_propriedades_numericas_por_concurso(self):
        query = "CREATE TABLE IF NOT EXISTS propriedades_numericas_por_concurso (\"Concurso\" INTEGER PRIMARY KEY, soma_dezenas INTEGER, pares INTEGER, impares INTEGER, primos INTEGER);" # Adicionar mais colunas de propriedades se existirem
        self._execute_query(query, commit=True); logger.debug("Tabela 'propriedades_numericas_por_concurso' OK.")

    def _create_table_analise_repeticao_concurso_anterior(self):
        query = "CREATE TABLE IF NOT EXISTS analise_repeticao_concurso_anterior (\"Concurso\" INTEGER PRIMARY KEY, \"Data\" TEXT, \"QtdDezenasRepetidas\" INTEGER, \"DezenasRepetidas\" TEXT);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'analise_repeticao_concurso_anterior' OK.")
    
    def _create_table_rank_geral_dezenas_por_frequencia(self):
        query = "CREATE TABLE IF NOT EXISTS rank_geral_dezenas_por_frequencia (Dezena INTEGER PRIMARY KEY, frequencia_total INTEGER, rank_geral INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'rank_geral_dezenas_por_frequencia' OK.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    try:
        # Este caminho é relativo ao local onde este script está. Ajuste se necessário.
        # Para testes, é melhor usar um caminho absoluto ou um db em memória.
        # db_m = DatabaseManager(db_path=':memory:')
        db_m = DatabaseManager(db_path='../Data/test_lotofacil.db') # Exemplo
        
        # Teste rápido para verificar se a tabela foi criada
        if db_m.table_exists('frequent_itemsets'):
            logger.info("Teste: Tabela 'frequent_itemsets' existe.")
        else:
            logger.error("Teste: Tabela 'frequent_itemsets' NÃO existe (erro esperado se não foi criada em _create_all_tables).")
        
        # Teste do load_dataframe
        # Criar uma tabela e dados de teste
        test_df = pd.DataFrame({'colA': [1, 2], 'colB': ['x', 'y']})
        db_m.save_dataframe(test_df, 'test_table_load', if_exists='replace')
        loaded_df = db_m.load_dataframe('test_table_load')
        if loaded_df is not None and not loaded_df.empty:
            logger.info(f"Teste load_dataframe: Carregado com sucesso. Conteúdo:\n{loaded_df}")
        else:
            logger.error("Teste load_dataframe: Falha ao carregar ou DataFrame vazio.")

        df_non_existent = db_m.load_dataframe('tabela_nao_existe')
        if df_non_existent is not None and df_non_existent.empty:
            logger.info("Teste load_dataframe para tabela inexistente: Retornou DataFrame vazio como esperado.")
        else:
            logger.error(f"Teste load_dataframe para tabela inexistente: Inesperado. Retornou: {df_non_existent}")

        db_m.close()
    except Exception as e_test:
        logger.error(f"Erro no exemplo de uso do DatabaseManager: {e_test}", exc_info=True)
    finally:
        # Limpar o arquivo de teste db se foi criado
        if os.path.exists('../Data/test_lotofacil.db'):
            os.remove('../Data/test_lotofacil.db')
            logger.info("Arquivo test_lotofacil.db removido.")