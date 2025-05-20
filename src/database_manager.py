# src/database_manager.py
import sqlite3
import pandas as pd
import logging
import os
from typing import List, Any, Tuple, Optional

# Importar Config para type hinting, mas a instância é geralmente passada ou importada como config_obj
# from .config import Config 

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        try:
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir): # Cria o diretório se não existir
                os.makedirs(db_dir)
                logger.info(f"Diretório do banco de dados criado: {db_dir}")
            self.connect()
            logger.info(f"Database Manager inicializado e conectado a: {db_path}")
        except sqlite3.Error as e:
            logger.error(f"Erro ao inicializar DatabaseManager para {db_path}: {e}", exc_info=True)
            raise

    def connect(self) -> None:
        """Estabelece a conexão com o banco de dados SQLite."""
        try:
            self.conn = sqlite3.connect(self.db_path, timeout=10) # Timeout para evitar locks longos
            self.conn.execute("PRAGMA foreign_keys = ON;")
            self.conn.execute("PRAGMA journal_mode = WAL;") # Melhor para concorrência
            self.cursor = self.conn.cursor()
            logger.debug(f"Conexão com o banco de dados {self.db_path} estabelecida.")
        except sqlite3.Error as e:
            logger.error(f"Erro ao conectar ao banco de dados {self.db_path}: {e}", exc_info=True)
            raise

    def close(self) -> None:
        """Fecha a conexão com o banco de dados."""
        if self.cursor:
            try: self.cursor.close()
            except sqlite3.Error as e: logger.error(f"Erro ao fechar cursor: {e}", exc_info=True)
            self.cursor = None
        if self.conn:
            try: self.conn.close()
            except sqlite3.Error as e: logger.error(f"Erro ao fechar conexão: {e}", exc_info=True)
            logger.debug(f"Conexão com o banco de dados {self.db_path} fechada.")
        self.conn = None

    def _ensure_connection(self):
        """Garante que uma conexão e cursor estejam ativos, tentando reconectar se necessário."""
        if not self.conn or not self.cursor:
            logger.warning("Conexão/cursor não ativos. Tentando reconectar...")
            self.connect() # connect() levanta exceção em caso de falha
            if not self.conn or not self.cursor: # Checagem dupla
                 logger.error("Falha crítica ao restabelecer conexão/cursor.")
                 raise sqlite3.Error("Falha ao restabelecer conexão com o banco de dados.")

    def _execute_ddl_query(self, query: str, params: Tuple = None) -> None:
        """Método interno para executar queries DDL (CREATE, ALTER, DROP)."""
        self._ensure_connection()
        try:
            logger.debug(f"Executando DDL: {query[:150]}...") # Log truncado
            self.cursor.execute(query, params or ())
            self.conn.commit()
            logger.debug("DDL comitada.")
        except sqlite3.Error as e:
            logger.error(f"Erro DDL: {query[:150]}... - {e}", exc_info=True)
            if self.conn: 
                try: self.conn.rollback()
                except Exception as rb_ex: logger.error(f"Erro no rollback após falha de DDL: {rb_ex}")
            raise

    def execute_query(self, query: str, params: Tuple = None) -> pd.DataFrame:
        """Executa uma query SELECT e retorna os resultados como um DataFrame Pandas."""
        self._ensure_connection()
        try:
            logger.debug(f"Executando SELECT: {query} com params: {params}")
            df = pd.read_sql_query(query, self.conn, params=params or ())
            logger.debug(f"SELECT executada. {len(df) if df is not None else 'Nenhuma'} linha(s).")
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro SELECT: {query} - {e}", exc_info=True)
            return pd.DataFrame()

    def save_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace') -> None:
        """Salva um DataFrame Pandas em uma tabela SQLite."""
        self._ensure_connection()
        if df is None:
            logger.warning(f"DataFrame para '{table_name}' é None. Nada salvo.")
            return
        try:
            logger.info(f"Salvando DataFrame em '{table_name}' (if_exists='{if_exists}', Linhas: {len(df)})")
            df.to_sql(table_name, self.conn, if_exists=if_exists, index=False, chunksize=1000)
            logger.info(f"DataFrame salvo em '{table_name}'.")
        except Exception as e:
            logger.error(f"Erro ao salvar DataFrame em '{table_name}': {e}", exc_info=True)
            raise

    def load_dataframe(self, table_name: str, query: Optional[str] = None, params: Optional[Tuple] = None) -> pd.DataFrame:
        """Carrega dados de uma tabela (ou query customizada) para um DataFrame Pandas."""
        final_query = query
        if not final_query:
            if not self.table_exists(table_name):
                logger.warning(f"Tabela '{table_name}' não existe. Retornando DataFrame vazio.")
                return pd.DataFrame()
            final_query = f"SELECT * FROM {table_name}"
        
        df = self.execute_query(final_query, params=params)
        if df is None: return pd.DataFrame() # execute_query retorna DataFrame vazio em erro
            
        log_source = f"tabela '{table_name}'" if not query else "query customizada"
        logger.info(f"DataFrame de {log_source} carregado com {len(df)} linhas.")
        return df

    def table_exists(self, table_name: str) -> bool:
        """Verifica se uma tabela existe no banco de dados."""
        self._ensure_connection()
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            exists = self.cursor.fetchone() is not None
            logger.debug(f"Tabela '{table_name}' {'existe.' if exists else 'não existe.'}")
            return exists
        except sqlite3.Error as e:
            logger.error(f"Erro ao verificar se tabela '{table_name}' existe: {e}", exc_info=True)
            return False

    def get_table_name_from_config(self, attr_name: str, default_name: str) -> str:
        """Auxiliar para obter nome de tabela do config_obj ou usar default."""
        from .config import config_obj # Importa config_obj aqui para acesso
        return getattr(config_obj, attr_name, default_name)

    # --- Métodos de Criação de Tabelas ---
    # (Usando constantes do config_obj onde disponível)

    def _create_table_draws(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('MAIN_DRAWS_TABLE_NAME', 'draws')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER PRIMARY KEY, {config_obj.DATE_COLUMN_NAME} TEXT,
            {", ".join([f'{col} INTEGER' for col in config_obj.BALL_NUMBER_COLUMNS])},
            {config_obj.DRAWN_NUMBERS_COLUMN_NAME} TEXT);"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_draw_results_flat(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('FLAT_DRAWS_TABLE_NAME', 'draw_results_flat')
        main_draws_table = self.get_table_name_from_config('MAIN_DRAWS_TABLE_NAME', 'draws')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL,
            PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}),
            FOREIGN KEY ({config_obj.CONTEST_ID_COLUMN_NAME}) REFERENCES {main_draws_table}({config_obj.CONTEST_ID_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")
            
    def _create_table_analysis_delays(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_DELAYS_TABLE_NAME', 'analysis_delays')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL,
            {config_obj.CURRENT_DELAY_COLUMN_NAME} INTEGER, {config_obj.MAX_DELAY_OBSERVED_COLUMN_NAME} INTEGER, {config_obj.AVG_DELAY_COLUMN_NAME} REAL,
            PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_frequency_overall(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_FREQUENCY_OVERALL_TABLE_NAME', 'analysis_frequency_overall')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL,
            {config_obj.FREQUENCY_COLUMN_NAME} INTEGER, {config_obj.RELATIVE_FREQUENCY_COLUMN_NAME} REAL,
            PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_recurrence_cdf(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_RECURRENCE_CDF_TABLE_NAME', 'analysis_recurrence_cdf')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL,
            {config_obj.RECURRENCE_CDF_COLUMN_NAME} REAL,
            PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_rank_trend_metrics(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_RANK_TREND_METRICS_TABLE_NAME', 'analysis_rank_trend_metrics')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL,
            {config_obj.RANK_SLOPE_COLUMN_NAME} REAL, {config_obj.TREND_STATUS_COLUMN_NAME} TEXT,
            {config_obj.CHUNK_TYPE_COLUMN_NAME} TEXT, {config_obj.CHUNK_SIZE_COLUMN_NAME} INTEGER,
            PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}, {config_obj.CHUNK_TYPE_COLUMN_NAME}, {config_obj.CHUNK_SIZE_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_cycle_status_dezenas(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_CYCLE_STATUS_DEZENAS_TABLE_NAME', 'analysis_cycle_status_dezenas')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL,
            {config_obj.CICLO_NUM_COLUMN_NAME} INTEGER, {config_obj.IS_MISSING_IN_CURRENT_CYCLE_COLUMN_NAME} INTEGER,
            PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_cycle_closing_propensity(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_CYCLE_CLOSING_PROPENSITY_TABLE_NAME', 'analysis_cycle_closing_propensity')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.DEZENA_COLUMN_NAME} INTEGER PRIMARY KEY, {config_obj.CYCLE_CLOSING_SCORE_COLUMN_NAME} REAL);"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_frequent_itemsets(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('FREQUENT_ITEMSETS_TABLE_NAME', 'frequent_itemsets')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.ITEMSET_STR_COLUMN_NAME} TEXT NOT NULL,
            {config_obj.SUPPORT_COLUMN_NAME} REAL NOT NULL, {config_obj.K_COLUMN_NAME} INTEGER NOT NULL,
            frequency_count INTEGER, PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.ITEMSET_STR_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_itemset_metrics(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_ITEMSET_METRICS_TABLE_NAME', 'analysis_itemset_metrics')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.ITEMSET_STR_COLUMN_NAME} TEXT NOT NULL,
            {config_obj.K_COLUMN_NAME} INTEGER, {config_obj.SUPPORT_COLUMN_NAME} REAL,
            {config_obj.CONFIDENCE_COLUMN_NAME} REAL, {config_obj.LIFT_COLUMN_NAME} REAL,
            {config_obj.ITEMSET_CURRENT_DELAY_COLUMN_NAME} INTEGER, {config_obj.ITEMSET_AVG_DELAY_COLUMN_NAME} REAL,
            {config_obj.ITEMSET_MAX_DELAY_COLUMN_NAME} INTEGER, {config_obj.ITEMSET_SCORE_COLUMN_NAME} REAL,
            last_occurrence_contest_id INTEGER, occurrences_draw_ids TEXT,
            PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.ITEMSET_STR_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_cycles_detail(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_CYCLES_DETAIL_TABLE_NAME', 'analysis_cycles_detail')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CICLO_NUM_COLUMN_NAME} INTEGER, concurso_inicio INTEGER, concurso_fim INTEGER, 
            duracao_concursos INTEGER, numeros_faltantes TEXT, qtd_faltantes INTEGER,
            PRIMARY KEY({config_obj.CICLO_NUM_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_cycles_summary(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_CYCLES_SUMMARY_TABLE_NAME', 'analysis_cycles_summary')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            summary_id INTEGER PRIMARY KEY DEFAULT 1, total_ciclos_fechados INTEGER, duracao_media_ciclo REAL, 
            duracao_min_ciclo INTEGER, duracao_max_ciclo INTEGER, duracao_mediana_ciclo REAL);"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analysis_cycle_progression_raw(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ANALYSIS_CYCLE_PROGRESSION_RAW_TABLE_NAME', 'analysis_cycle_progression_raw')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER, {config_obj.DATE_COLUMN_NAME} TEXT, ciclo_num_associado INTEGER, 
            dezenas_sorteadas_neste_concurso TEXT, numeros_que_faltavam_antes_deste_concurso TEXT, 
            qtd_faltavam_antes_deste_concurso INTEGER, dezenas_apuradas_neste_concurso TEXT, 
            qtd_apuradas_neste_concurso INTEGER, numeros_faltantes_apos_este_concurso TEXT, 
            qtd_faltantes_apos_este_concurso INTEGER, ciclo_fechou_neste_concurso INTEGER,
            PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, ciclo_num_associado));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_propriedades_numericas_por_concurso(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('PROPRIEDADES_NUMERICAS_POR_CONCURSO_TABLE_NAME', 'propriedades_numericas_por_concurso')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER PRIMARY KEY, 
            soma_dezenas INTEGER, pares INTEGER, impares INTEGER, primos INTEGER);"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_analise_repeticao_concurso_anterior(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('REPETICAO_CONCURSO_ANTERIOR_TABLE_NAME', 'analise_repeticao_concurso_anterior')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER PRIMARY KEY, 
            {config_obj.DATE_COLUMN_NAME} TEXT, QtdDezenasRepetidas INTEGER, DezenasRepetidas TEXT);"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_chunk_metrics(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('CHUNK_METRICS_TABLE_NAME', 'chunk_metrics')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CHUNK_TYPE_COLUMN_NAME} TEXT, {config_obj.CHUNK_SIZE_COLUMN_NAME} INTEGER,
            chunk_start_draw_id INTEGER, chunk_end_draw_id INTEGER, {config_obj.DEZENA_COLUMN_NAME} INTEGER,
            frequency_in_chunk_abs INTEGER, frequency_in_chunk_rel REAL,
            current_delay_in_chunk INTEGER, max_delay_in_chunk INTEGER, avg_delay_in_chunk REAL,
            delay_std_dev REAL, occurrence_std_dev REAL,
            PRIMARY KEY ({config_obj.CHUNK_TYPE_COLUMN_NAME}, {config_obj.CHUNK_SIZE_COLUMN_NAME}, chunk_start_draw_id, {config_obj.DEZENA_COLUMN_NAME}));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_draw_position_frequency(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('DRAW_POSITION_FREQUENCY_TABLE_NAME', 'draw_position_frequency')
        cols_posicao = ", ".join([f"Posicao_{i} INTEGER DEFAULT 0" for i in range(1, 16)])
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({config_obj.DEZENA_COLUMN_NAME} INTEGER PRIMARY KEY, {cols_posicao});"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_geral_ma_frequency(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('GERAL_MA_FREQUENCY_TABLE_NAME', 'geral_ma_frequency')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL, Janela INTEGER NOT NULL,
            MA_Frequencia REAL, PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}, Janela));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_geral_ma_delay(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('GERAL_MA_DELAY_TABLE_NAME', 'geral_ma_delay')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.CONTEST_ID_COLUMN_NAME} INTEGER NOT NULL, {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL, Janela INTEGER NOT NULL,
            MA_Atraso REAL, PRIMARY KEY ({config_obj.CONTEST_ID_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}, Janela));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_association_rules(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('ASSOCIATION_RULES_TABLE_NAME', 'association_rules')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            antecedents_str TEXT NOT NULL, consequents_str TEXT NOT NULL,
            antecedent_support REAL, consequent_support REAL, support REAL,
            confidence REAL, lift REAL, leverage REAL, conviction REAL,
            PRIMARY KEY (antecedents_str, consequents_str));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_grid_line_distribution(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('GRID_LINE_DISTRIBUTION_TABLE_NAME', 'grid_line_distribution')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            Linha TEXT NOT NULL, Qtd_Dezenas_Sorteadas INTEGER NOT NULL,
            Frequencia_Absoluta INTEGER NOT NULL, Frequencia_Relativa REAL NOT NULL,
            PRIMARY KEY (Linha, Qtd_Dezenas_Sorteadas));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_grid_column_distribution(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('GRID_COLUMN_DISTRIBUTION_TABLE_NAME', 'grid_column_distribution')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            Coluna TEXT NOT NULL, Qtd_Dezenas_Sorteadas INTEGER NOT NULL,
            Frequencia_Absoluta INTEGER NOT NULL, Frequencia_Relativa REAL NOT NULL,
            PRIMARY KEY (Coluna, Qtd_Dezenas_Sorteadas));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")
    
    def _create_table_statistical_tests_results(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('STATISTICAL_TESTS_RESULTS_TABLE_NAME', 'statistical_tests_results')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            Test_ID INTEGER PRIMARY KEY AUTOINCREMENT, Test_Name TEXT NOT NULL,
            Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, Chi2_Statistic REAL,
            P_Value REAL, Degrees_Freedom INTEGER, Alpha_Level REAL DEFAULT 0.05,
            Conclusion TEXT, Parameters TEXT, Notes TEXT);"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_monthly_number_frequency(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('MONTHLY_NUMBER_FREQUENCY_TABLE_NAME', 'monthly_number_frequency')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            {config_obj.DEZENA_COLUMN_NAME} INTEGER NOT NULL, Mes INTEGER NOT NULL,
            Frequencia_Absoluta_Total_Mes INTEGER, Total_Sorteios_Considerados_Mes INTEGER,
            Frequencia_Relativa_Mes REAL, PRIMARY KEY ({config_obj.DEZENA_COLUMN_NAME}, Mes));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_monthly_draw_properties_summary(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('MONTHLY_DRAW_PROPERTIES_TABLE_NAME', 'monthly_draw_properties_summary')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            Mes INTEGER PRIMARY KEY, Total_Sorteios_Mes INTEGER, Soma_Media_Mensal REAL,
            Media_Pares_Mensal REAL, Media_Impares_Mensal REAL, Media_Primos_Mensal REAL);"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")
            
    def _create_table_sequence_metrics(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('SEQUENCE_METRICS_TABLE_NAME', 'sequence_metrics')
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            sequence_description TEXT, sequence_type TEXT, length INTEGER, step INTEGER,
            specific_sequence TEXT, frequency_count INTEGER, support REAL,
            PRIMARY KEY (sequence_type, length, step, specific_sequence));"""
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_ciclo_metric_frequency(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('CYCLE_METRIC_FREQUENCY_TABLE_NAME', 'ciclo_metric_frequency')
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({config_obj.CICLO_NUM_COLUMN_NAME} INTEGER, {config_obj.DEZENA_COLUMN_NAME} INTEGER, frequencia_no_ciclo INTEGER, PRIMARY KEY ({config_obj.CICLO_NUM_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")
    
    def _create_table_ciclo_metric_atraso_medio(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('CYCLE_METRIC_ATRASO_MEDIO_TABLE_NAME', 'ciclo_metric_atraso_medio')
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({config_obj.CICLO_NUM_COLUMN_NAME} INTEGER, {config_obj.DEZENA_COLUMN_NAME} INTEGER, atraso_medio_no_ciclo REAL, PRIMARY KEY ({config_obj.CICLO_NUM_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_ciclo_metric_atraso_maximo(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('CYCLE_METRIC_ATRASO_MAXIMO_TABLE_NAME', 'ciclo_metric_atraso_maximo')
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({config_obj.CICLO_NUM_COLUMN_NAME} INTEGER, {config_obj.DEZENA_COLUMN_NAME} INTEGER, atraso_maximo_no_ciclo INTEGER, PRIMARY KEY ({config_obj.CICLO_NUM_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_ciclo_metric_atraso_final(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('CYCLE_METRIC_ATRASO_FINAL_TABLE_NAME', 'ciclo_metric_atraso_final')
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({config_obj.CICLO_NUM_COLUMN_NAME} INTEGER, {config_obj.DEZENA_COLUMN_NAME} INTEGER, atraso_final_no_ciclo INTEGER, PRIMARY KEY ({config_obj.CICLO_NUM_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_ciclo_rank_frequency(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('CYCLE_RANK_FREQUENCY_TABLE_NAME', 'ciclo_rank_frequency')
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({config_obj.CICLO_NUM_COLUMN_NAME} INTEGER, {config_obj.DEZENA_COLUMN_NAME} INTEGER, frequencia_no_ciclo INTEGER, rank_freq_no_ciclo INTEGER, PRIMARY KEY ({config_obj.CICLO_NUM_COLUMN_NAME}, {config_obj.DEZENA_COLUMN_NAME}));"
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_table_ciclo_group_metrics(self) -> None:
        from .config import config_obj
        table_name = self.get_table_name_from_config('CYCLE_GROUP_METRICS_TABLE_NAME', 'ciclo_group_metrics')
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({config_obj.CICLO_NUM_COLUMN_NAME} INTEGER PRIMARY KEY, avg_pares_no_ciclo REAL, avg_impares_no_ciclo REAL, avg_primos_no_ciclo REAL);"
        if not self.table_exists(table_name): self._execute_ddl_query(query); logger.debug(f"Tabela '{table_name}' verificada/criada.")

    def _create_all_tables(self) -> None:
        """Verifica e cria todas as tabelas conhecidas se não existirem."""
        logger.info("Verificando e criando todas as tabelas do banco de dados se não existirem...")
        
        creation_methods = [
            self._create_table_draws, self._create_table_draw_results_flat,
            self._create_table_analysis_delays, self._create_table_analysis_frequency_overall,
            self._create_table_analysis_recurrence_cdf, self._create_table_analysis_rank_trend_metrics,
            self._create_table_analysis_cycle_status_dezenas, self._create_table_analysis_cycle_closing_propensity,
            self._create_table_frequent_itemsets, self._create_table_analysis_itemset_metrics,
            self._create_table_analysis_cycles_detail, self._create_table_analysis_cycles_summary,
            self._create_table_analysis_cycle_progression_raw,
            self._create_table_propriedades_numericas_por_concurso,
            self._create_table_analise_repeticao_concurso_anterior,
            self._create_table_chunk_metrics,
            self._create_table_draw_position_frequency,
            self._create_table_geral_ma_frequency, self._create_table_geral_ma_delay,
            self._create_table_association_rules, self._create_table_grid_line_distribution,
            self._create_table_grid_column_distribution, self._create_table_statistical_tests_results,
            self._create_table_monthly_number_frequency, self._create_table_monthly_draw_properties_summary,
            self._create_table_sequence_metrics,
            self._create_table_ciclo_metric_frequency, self._create_table_ciclo_metric_atraso_medio,
            self._create_table_ciclo_metric_atraso_maximo, self._create_table_ciclo_metric_atraso_final,
            self._create_table_ciclo_rank_frequency, self._create_table_ciclo_group_metrics,
        ]
        
        for method_func in creation_methods:
            try:
                method_func()
            except Exception as e_create:
                method_name = method_func.__name__ if hasattr(method_func, '__name__') else 'desconhecido'
                logger.error(f"Erro ao tentar executar método de criação de tabela '{method_name}': {e_create}", exc_info=True)
        logger.info("Verificação e criação de tabelas (definidas na lista) concluída.")

    def __enter__(self):
        if not self.conn: self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Bloco if __name__ == '__main__' para teste direto
if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.DEBUG, 
            format='%(asctime)s - %(name)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s',
            handlers=[logging.StreamHandler()]
        )
    
    db_test_path_in_memory = ":memory:" 
    logger.info(f"Usando banco de dados de teste em: {db_test_path_in_memory}")
    
    db_m = None
    try:
        # Importar config_obj aqui para o contexto de teste
        from src.config import config_obj
        if not config_obj:
            raise ImportError("config_obj não pôde ser importado ou é None para o teste do DatabaseManager")

        db_m = DatabaseManager(db_path=db_test_path_in_memory)
        db_m._create_all_tables() # Chama o método que executa todos os _create_table_*
        
        logger.info(f"Tabelas no banco de dados após _create_all_tables: {db_m.get_table_names()}")

        # Teste simples de save/load
        test_df_data = {'col1': [1, 2, 3], 'col2': ['x', 'y', 'z']}
        test_df = pd.DataFrame(test_df_data)
        db_m.save_dataframe(test_df, 'test_table_for_manager')
        loaded_df = db_m.load_dataframe('test_table_for_manager')
        if loaded_df is not None and not loaded_df.empty and loaded_df.equals(test_df):
            logger.info(f"Teste de save/load para 'test_table_for_manager' BEM-SUCEDIDO.")
        else:
            logger.error(f"Falha no teste de save/load para 'test_table_for_manager'. Carregado:\n{loaded_df}")

    except Exception as e_test:
        logger.error(f"Erro no script de teste do DatabaseManager: {e_test}", exc_info=True)
    finally:
        if db_m:
            db_m.close()