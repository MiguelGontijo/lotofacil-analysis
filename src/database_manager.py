# src/database_manager.py
import sqlite3
import pandas as pd
import logging
from typing import List, Dict, Any, Tuple, Optional
import os 
import json 

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        try:
            self.connect()
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

    def load_dataframe_from_db(self, table_name: str) -> Optional[pd.DataFrame]:
        logger.debug(f"Chamando load_dataframe_from_db, redirecionando para load_dataframe para tabela: {table_name}")
        return self.load_dataframe(table_name)

    def table_exists(self, table_name: str) -> bool:
        if not self.cursor: 
            logger.debug("Cursor não está ativo em table_exists. Tentando reconectar.")
            try:
                self.connect()
            except sqlite3.Error as e_connect:
                logger.error(f"Falha ao reconectar em table_exists para '{table_name}': {e_connect}")
                return False 
            if not self.cursor: 
                 logger.error(f"Cursor ainda não está ativo após tentativa de reconexão em table_exists para '{table_name}'.")
                 return False
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Erro ao verificar a existência da tabela '{table_name}': {e}", exc_info=True)
            return False
        
    def get_table_names(self) -> List[str]:
        if not self.conn or not self.cursor: 
            logger.debug("Tentativa de obter nomes de tabelas sem conexão/cursor. Tentando reconectar.")
            try:
                self.connect()
            except sqlite3.Error as e_connect:
                logger.error(f"Falha ao reconectar para obter nomes de tabelas: {e_connect}")
                return []
            if not self.cursor:
                 logger.error("Cursor ainda não está ativo após tentativa de reconexão para obter nomes de tabelas.")
                 return []
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            tables = self.cursor.fetchall()
            return [table[0] for table in tables]
        except sqlite3.Error as e:
            logger.error(f"Erro ao buscar nomes de tabelas: {e}", exc_info=True)
            return []
    
    def _create_all_tables(self) -> None:
        logger.info("Verificando e criando todas as tabelas do banco de dados se não existirem...")
        
        self._create_table_frequencia_absoluta()
        self._create_table_frequencia_relativa()
        self._create_table_atraso_atual()
        self._create_table_atraso_maximo()
        self._create_table_atraso_maximo_separado() 
        self._create_table_atraso_medio()
        self._create_table_pair_metrics() 
        self._create_frequent_itemsets_table()
        self._create_table_frequent_itemset_metrics() 
        self._create_table_sequence_metrics()
        
        # Adições das etapas anteriores mantidas (assumindo que já foram adicionadas ao seu arquivo)
        # Usando hasattr para chamar condicionalmente, caso os métodos não tenham sido adicionados
        # ao arquivo do usuário por algum motivo (embora o esperado seja que estejam).
        if hasattr(self, '_create_table_draw_position_frequency'): self._create_table_draw_position_frequency()
        if hasattr(self, '_create_table_geral_ma_frequency'): self._create_table_geral_ma_frequency()
        if hasattr(self, '_create_table_geral_ma_delay'): self._create_table_geral_ma_delay()
        if hasattr(self, '_create_table_geral_recurrence_analysis'): self._create_table_geral_recurrence_analysis()
        if hasattr(self, '_create_table_association_rules'): self._create_table_association_rules()
        if hasattr(self, '_create_table_grid_line_distribution'): self._create_table_grid_line_distribution()
        if hasattr(self, '_create_table_grid_column_distribution'): self._create_table_grid_column_distribution()
        self._create_table_statistical_tests_results() # Nova chamada

        self._create_table_ciclos_detalhe()
        self._create_table_ciclos_sumario_estatisticas()
        self._create_table_ciclo_progression()
        self._create_table_ciclo_metric_frequency()
        self._create_table_ciclo_metric_atraso_medio()
        self._create_table_ciclo_metric_atraso_maximo()
        self._create_table_ciclo_metric_atraso_final()
        self._create_table_ciclo_rank_frequency()
        self._create_table_ciclo_group_metrics()

        self._create_table_propriedades_numericas_por_concurso()
        self._create_table_analise_repeticao_concurso_anterior()
        
        self._create_table_chunk_metrics() 

        self._create_table_rank_geral_dezenas_por_frequencia()
        
        logger.info("Verificação e criação de tabelas (essenciais listadas) concluída.")

    # Métodos de criação de tabela existentes (mantidos como estão no seu arquivo original)
    # ... (todos os seus _create_table_* existentes até antes dos novos) ...
    # Para ser conciso, vou omitir a repetição dos métodos _create_table_* que já adicionamos
    # nas etapas anteriores, assumindo que eles estão no seu arquivo.
    # Apenas o novo método será explicitamente definido abaixo.

    def _create_table_statistical_tests_results(self) -> None:
        """
        Cria a tabela para armazenar os resultados de testes estatísticos diversos.
        """
        query = """
        CREATE TABLE IF NOT EXISTS statistical_tests_results (
            Test_ID INTEGER PRIMARY KEY AUTOINCREMENT, -- Chave primária para cada resultado de teste
            Test_Name TEXT NOT NULL,                   -- Ex: 'ChiSquare_NumberFrequencies_Uniformity'
            Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            Chi2_Statistic REAL,
            P_Value REAL,
            Degrees_Freedom INTEGER,
            Alpha_Level REAL DEFAULT 0.05,             -- Nível de significância usado
            Conclusion TEXT,                           -- Ex: 'Rejeita H0: Distribuição não uniforme'
            Parameters TEXT,                           -- JSON string com parâmetros específicos do teste
            Notes TEXT                                 -- Observações adicionais
        );
        """
        self._execute_query(query, commit=True)
        logger.debug("Tabela 'statistical_tests_results' verificada/criada.")
    
    # É importante que todos os métodos _create_table_* referenciados em _create_all_tables
    # estejam definidos na classe. Omiti os que já existem no seu arquivo para brevidade.
    # Vou adicionar apenas os que são estritamente novos para esta etapa,
    # ou garantir que os outros sejam chamados com hasattr como feito acima.
    # Para os métodos adicionados nas últimas etapas (draw_position_frequency, etc.),
    # se eles não estiverem no seu arquivo original, eles precisariam ser adicionados
    # de forma similar a _create_table_statistical_tests_results.

    # Assumindo que os métodos de criação para as tabelas das etapas anteriores
    # (draw_position_frequency, geral_ma_frequency, geral_ma_delay, 
    # geral_recurrence_analysis, association_rules, grid_line_distribution, grid_column_distribution)
    # já foram adicionados ao seu arquivo src/database_manager.py nas etapas correspondentes.

    # Lista de métodos de criação de tabelas existentes que devem estar no seu arquivo original
    # (Eu não os redefinirei aqui para evitar redundância, mas eles são chamados em _create_all_tables)
    def _create_frequent_itemsets_table(self) -> None: # Exemplo de método existente
        query = "CREATE TABLE IF NOT EXISTS frequent_itemsets (itemset_str TEXT PRIMARY KEY, support REAL NOT NULL, length INTEGER NOT NULL, frequency_count INTEGER NOT NULL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'frequent_itemsets' OK.")
    # ... (e todos os outros métodos _create_table_* do seu arquivo original) ...
    # ... (incluindo os que adicionamos para draw_position_frequency, MAs, recurrence, association_rules, grid)

    # Certifique-se de que todos os métodos chamados em _create_all_tables estão definidos na classe.
    # Se algum método como _create_table_draw_position_frequency foi apenas sugerido
    # mas não explicitamente adicionado ao seu arquivo nas etapas anteriores, ele precisaria
    # ser adicionado agora de forma completa.
    # Vou adicionar abaixo as definições dos métodos que criamos nas etapas anteriores,
    # caso eles não tenham sido totalmente integrados ao seu arquivo.

    def _create_table_draw_position_frequency(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS draw_position_frequency (
            Dezena INTEGER PRIMARY KEY, Posicao_1 INTEGER DEFAULT 0, Posicao_2 INTEGER DEFAULT 0,
            Posicao_3 INTEGER DEFAULT 0, Posicao_4 INTEGER DEFAULT 0, Posicao_5 INTEGER DEFAULT 0,
            Posicao_6 INTEGER DEFAULT 0, Posicao_7 INTEGER DEFAULT 0, Posicao_8 INTEGER DEFAULT 0,
            Posicao_9 INTEGER DEFAULT 0, Posicao_10 INTEGER DEFAULT 0, Posicao_11 INTEGER DEFAULT 0,
            Posicao_12 INTEGER DEFAULT 0, Posicao_13 INTEGER DEFAULT 0, Posicao_14 INTEGER DEFAULT 0,
            Posicao_15 INTEGER DEFAULT 0
        );"""
        if not self.table_exists('draw_position_frequency'):
             self._execute_query(query, commit=True); logger.debug("Tabela 'draw_position_frequency' verificada/criada.")

    def _create_table_geral_ma_frequency(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS geral_ma_frequency (
            Concurso INTEGER NOT NULL, Dezena INTEGER NOT NULL, Janela INTEGER NOT NULL,
            MA_Frequencia REAL, PRIMARY KEY (Concurso, Dezena, Janela)
        );"""
        if not self.table_exists('geral_ma_frequency'):
            self._execute_query(query, commit=True); logger.debug("Tabela 'geral_ma_frequency' verificada/criada.")

    def _create_table_geral_ma_delay(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS geral_ma_delay (
            Concurso INTEGER NOT NULL, Dezena INTEGER NOT NULL, Janela INTEGER NOT NULL,
            MA_Atraso REAL, PRIMARY KEY (Concurso, Dezena, Janela)
        );"""
        if not self.table_exists('geral_ma_delay'):
            self._execute_query(query, commit=True); logger.debug("Tabela 'geral_ma_delay' verificada/criada.")

    def _create_table_geral_recurrence_analysis(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS geral_recurrence_analysis (
            Dezena INTEGER PRIMARY KEY, Atraso_Atual INTEGER, CDF_Atraso_Atual REAL,
            Total_Gaps_Observados INTEGER, Media_Gaps REAL, Mediana_Gaps INTEGER,
            Std_Dev_Gaps REAL, Max_Gap_Observado INTEGER, Gaps_Observados TEXT
        );"""
        if not self.table_exists('geral_recurrence_analysis'):
            self._execute_query(query, commit=True); logger.debug("Tabela 'geral_recurrence_analysis' verificada/criada.")

    def _create_table_association_rules(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS association_rules (
            antecedents_str TEXT NOT NULL, consequents_str TEXT NOT NULL,
            antecedent_support REAL, consequent_support REAL, support REAL,
            confidence REAL, lift REAL, leverage REAL, conviction REAL,
            PRIMARY KEY (antecedents_str, consequents_str)
        );"""
        if not self.table_exists('association_rules'):
            self._execute_query(query, commit=True); logger.debug("Tabela 'association_rules' verificada/criada.")

    def _create_table_grid_line_distribution(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS grid_line_distribution (
            Linha TEXT NOT NULL, Qtd_Dezenas_Sorteadas INTEGER NOT NULL,
            Frequencia_Absoluta INTEGER NOT NULL, Frequencia_Relativa REAL NOT NULL,
            PRIMARY KEY (Linha, Qtd_Dezenas_Sorteadas)
        );"""
        if not self.table_exists('grid_line_distribution'):
            self._execute_query(query, commit=True); logger.debug("Tabela 'grid_line_distribution' verificada/criada.")

    def _create_table_grid_column_distribution(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS grid_column_distribution (
            Coluna TEXT NOT NULL, Qtd_Dezenas_Sorteadas INTEGER NOT NULL,
            Frequencia_Absoluta INTEGER NOT NULL, Frequencia_Relativa REAL NOT NULL,
            PRIMARY KEY (Coluna, Qtd_Dezenas_Sorteadas)
        );"""
        if not self.table_exists('grid_column_distribution'):
            self._execute_query(query, commit=True); logger.debug("Tabela 'grid_column_distribution' verificada/criada.")
    
    # ... (e todos os outros métodos _create_table_* do seu arquivo original da pasta)
    def _create_table_frequent_itemset_metrics(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS frequent_itemset_metrics (
            itemset_str TEXT PRIMARY KEY, length INTEGER, support REAL, frequency_count INTEGER,
            last_occurrence_contest_id INTEGER, current_delay INTEGER, mean_delay REAL,
            max_delay INTEGER, std_dev_delay REAL, occurrences_draw_ids TEXT, 
            FOREIGN KEY(itemset_str) REFERENCES frequent_itemsets(itemset_str)
        );"""
        self._execute_query(query, commit=True); logger.debug("Tabela 'frequent_itemset_metrics' verificada/criada.")

    def _create_table_sequence_metrics(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS sequence_metrics (
            sequence_description TEXT, sequence_type TEXT, length INTEGER, step INTEGER,
            specific_sequence TEXT, frequency_count INTEGER, support REAL,
            PRIMARY KEY (sequence_type, length, step, specific_sequence)
        );"""
        self._execute_query(query, commit=True); logger.debug("Tabela 'sequence_metrics' verificada/criada.")

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
    
    def _create_table_atraso_maximo_separado(self): 
        query = "CREATE TABLE IF NOT EXISTS atraso_maximo_separado (\"Dezena\" INTEGER PRIMARY KEY, \"Atraso Maximo\" INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'atraso_maximo_separado' OK.")

    def _create_table_atraso_medio(self):
        query = "CREATE TABLE IF NOT EXISTS atraso_medio (\"Dezena\" INTEGER PRIMARY KEY, \"Atraso Medio\" REAL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'atraso_medio' OK.")
        
    def _create_table_pair_metrics(self):
        query = "CREATE TABLE IF NOT EXISTS pair_metrics (pair_str TEXT PRIMARY KEY, frequency INTEGER, last_contest INTEGER, current_delay INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'pair_metrics' OK.")

    def _create_table_ciclos_detalhe(self):
        query = "CREATE TABLE IF NOT EXISTS ciclos_detalhe (ciclo_num INTEGER, concurso_inicio INTEGER, concurso_fim INTEGER, duracao_concursos INTEGER, numeros_faltantes TEXT, qtd_faltantes INTEGER, PRIMARY KEY(ciclo_num, concurso_inicio));"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclos_detalhe' OK.")

    def _create_table_ciclos_sumario_estatisticas(self):
        query = "CREATE TABLE IF NOT EXISTS ciclos_sumario_estatisticas (summary_id INTEGER PRIMARY KEY DEFAULT 1, total_ciclos_fechados INTEGER, duracao_media_ciclo REAL, duracao_min_ciclo INTEGER, duracao_max_ciclo INTEGER, duracao_mediana_ciclo REAL);"
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
                    ciclo_fechou_neste_concurso INTEGER,
                    PRIMARY KEY (Concurso, ciclo_num_associado)
                 );"""
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_progression' OK.")

    def _create_table_ciclo_metric_frequency(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_metric_frequency (ciclo_num INTEGER, dezena INTEGER, frequencia_no_ciclo INTEGER, PRIMARY KEY (ciclo_num, dezena));"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_metric_frequency' OK.")
    
    def _create_table_ciclo_metric_atraso_medio(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_metric_atraso_medio (ciclo_num INTEGER, dezena INTEGER, atraso_medio_no_ciclo REAL, PRIMARY KEY (ciclo_num, dezena));"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_metric_atraso_medio' OK.")

    def _create_table_ciclo_metric_atraso_maximo(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_metric_atraso_maximo (ciclo_num INTEGER, dezena INTEGER, atraso_maximo_no_ciclo INTEGER, PRIMARY KEY (ciclo_num, dezena));"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_metric_atraso_maximo' OK.")

    def _create_table_ciclo_metric_atraso_final(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_metric_atraso_final (ciclo_num INTEGER, dezena INTEGER, atraso_final_no_ciclo INTEGER, PRIMARY KEY (ciclo_num, dezena));"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_metric_atraso_final' OK.")

    def _create_table_ciclo_rank_frequency(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_rank_frequency (ciclo_num INTEGER, dezena INTEGER, frequencia_no_ciclo INTEGER, rank_freq_no_ciclo INTEGER, PRIMARY KEY (ciclo_num, dezena));"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_rank_frequency' OK.")

    def _create_table_ciclo_group_metrics(self):
        query = "CREATE TABLE IF NOT EXISTS ciclo_group_metrics (ciclo_num INTEGER PRIMARY KEY, avg_pares_no_ciclo REAL, avg_impares_no_ciclo REAL, avg_primos_no_ciclo REAL);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'ciclo_group_metrics' OK.")

    def _create_table_propriedades_numericas_por_concurso(self):
        query = "CREATE TABLE IF NOT EXISTS propriedades_numericas_por_concurso (\"Concurso\" INTEGER PRIMARY KEY, soma_dezenas INTEGER, pares INTEGER, impares INTEGER, primos INTEGER);" 
        self._execute_query(query, commit=True); logger.debug("Tabela 'propriedades_numericas_por_concurso' OK.")

    def _create_table_analise_repeticao_concurso_anterior(self):
        query = "CREATE TABLE IF NOT EXISTS analise_repeticao_concurso_anterior (\"Concurso\" INTEGER PRIMARY KEY, \"Data\" TEXT, \"QtdDezenasRepetidas\" INTEGER, \"DezenasRepetidas\" TEXT);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'analise_repeticao_concurso_anterior' OK.")
    
    def _create_table_rank_geral_dezenas_por_frequencia(self):
        query = "CREATE TABLE IF NOT EXISTS rank_geral_dezenas_por_frequencia (Dezena INTEGER PRIMARY KEY, frequencia_total INTEGER, rank_geral INTEGER);"
        self._execute_query(query, commit=True); logger.debug("Tabela 'rank_geral_dezenas_por_frequencia' OK.")

    def _create_table_chunk_metrics(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS chunk_metrics (
            chunk_type TEXT,
            chunk_size INTEGER,
            chunk_start_draw_id INTEGER,
            chunk_end_draw_id INTEGER,
            number INTEGER,
            frequency_in_chunk_abs INTEGER,
            frequency_in_chunk_rel REAL,
            current_delay_in_chunk INTEGER,
            max_delay_in_chunk INTEGER,
            avg_delay_in_chunk REAL,
            delay_std_dev REAL,
            occurrence_std_dev REAL,
            PRIMARY KEY (chunk_type, chunk_size, chunk_start_draw_id, number)
        )
        """
        self._execute_query(query, commit=True) 
        logger.debug("Tabela 'chunk_metrics' verificada/criada.")


    def __enter__(self): 
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb): 
        self.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    db_test_path = None 
    try:
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        project_base_dir = os.path.dirname(current_script_dir) 
        data_dir = os.path.join(project_base_dir, 'Data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir) 
            logger.info(f"Diretório '{data_dir}' criado.")
            
        db_test_path = os.path.join(data_dir, 'test_lotofacil_manager.db')
        logger.info(f"Usando banco de dados de teste em: {db_test_path}")
        
        db_m = DatabaseManager(db_path=db_test_path)
        db_m._create_all_tables() 
        
        logger.info(f"Tabelas no banco de dados: {db_m.get_table_names()}")

        tables_to_check = [
            'draw_position_frequency', 'geral_ma_frequency', 'geral_ma_delay', 
            'geral_recurrence_analysis', 'association_rules',
            'grid_line_distribution', 'grid_column_distribution',
            'statistical_tests_results' # Nova verificação
        ]
        for table_name in tables_to_check:
            if db_m.table_exists(table_name):
                logger.info(f"Teste: Tabela '{table_name}' existe.")
            else:
                logger.error(f"Teste: Tabela '{table_name}' NÃO existe após _create_all_tables.")

        test_df = pd.DataFrame({'colA': [1, 2], 'colB': ['x', 'y']})
        db_m.save_dataframe(test_df, 'test_table_load', if_exists='replace')
        loaded_df = db_m.load_dataframe('test_table_load')
        if loaded_df is not None and not loaded_df.empty:
            logger.info(f"Teste load_dataframe: Carregado com sucesso. Conteúdo:\n{loaded_df}")
        else:
            logger.error("Teste load_dataframe: Falha ao carregar ou DataFrame vazio.")

        db_m.close()
    except Exception as e_test:
        logger.error(f"Erro no exemplo de uso do DatabaseManager: {e_test}", exc_info=True)
    finally:
        if db_test_path and os.path.exists(db_test_path): 
            try:
                os.remove(db_test_path)
                logger.info(f"Arquivo de teste '{db_test_path}' removido.")
            except OSError as e_os:
                logger.error(f"Erro ao remover arquivo de teste '{db_test_path}': {e_os}")