# src/analysis_aggregator.py
import pandas as pd
from typing import Optional, List, Dict, Any
import logging
from sklearn.preprocessing import MinMaxScaler

# Removido logging.basicConfig para permitir configuração externa
logger = logging.getLogger(__name__)

# Importar o objeto de configuração diretamente e o tipo Config para type hinting
from .database_manager import DatabaseManager
from .config import config_obj, Config # Importando o config_obj global e a classe Config

class AnalysisAggregator:
    def __init__(self, db_manager: DatabaseManager,
                 config_instance: Optional[Config] = None): # Recebe uma instância de Config
        self.db_manager = db_manager
        # Prioriza a instância de config passada, caso contrário usa o config_obj global
        # Isso é útil para testes onde você pode querer injetar uma configuração mockada.
        self.config_access: Config = config_instance if config_instance is not None else config_obj
        
        if not isinstance(self.config_access, Config):
            logger.error(f"AnalysisAggregator recebeu um objeto de configuração inválido. Esperava uma instância da classe 'Config', recebeu {type(self.config_access)}. Revertendo para config_obj global.")
            self.config_access = config_obj # Fallback para o global em caso de erro grave
            if not isinstance(self.config_access, Config): # Checagem final
                 logger.critical("Falha crítica: config_obj global também não é uma instância de Config. O Aggregator pode não funcionar corretamente.")
                 # Em um cenário real, poderia levantar uma exceção aqui.
                 # Por agora, ele tentará continuar, mas provavelmente falhará ao acessar atributos.

        self._all_dezenas_list = self.config_access.ALL_NUMBERS
        self._default_recent_window = self.config_access.AGGREGATOR_DEFAULT_RECENT_WINDOW # Nova constante do config.py
        
        # Nomes das tabelas agora são lidos diretamente das constantes em config_obj
        self.table_names = {
            'draws': self.config_access.MAIN_DRAWS_TABLE_NAME,
            'draws_flat': self.config_access.FLAT_DRAWS_TABLE_NAME,
            'delays': self.config_access.ANALYSIS_DELAYS_TABLE_NAME,
            'frequency_overall': self.config_access.ANALYSIS_FREQUENCY_OVERALL_TABLE_NAME,
            'recurrence_cdf': self.config_access.ANALYSIS_RECURRENCE_CDF_TABLE_NAME,
            'rank_trends': self.config_access.ANALYSIS_RANK_TREND_METRICS_TABLE_NAME,
            'cycle_status': self.config_access.ANALYSIS_CYCLE_STATUS_DEZENAS_TABLE_NAME,
            'cycle_closing_propensity': self.config_access.ANALYSIS_CYCLE_CLOSING_PROPENSITY_TABLE_NAME,
            'itemset_metrics': self.config_access.ANALYSIS_ITEMSET_METRICS_TABLE_NAME
        }

        # Colunas que o método principal get_historical_metrics_for_dezenas tentará preencher.
        # A chave 'source_table' agora reflete os nomes padronizados.
        self.metric_configs = {
            'current_delay': {'default': 0, 'type': int, 'source_table': self.table_names['delays']},
            'max_delay_observed': {'default': 0, 'type': int, 'source_table': self.table_names['delays']},
            'avg_delay': {'default': 0.0, 'type': float, 'source_table': self.table_names['delays']},
            'overall_frequency': {'default': 0, 'type': int, 'source_table': self.table_names['frequency_overall']},
            'overall_relative_frequency': {'default': 0.0, 'type': float, 'source_table': self.table_names['frequency_overall']},
            f'recent_frequency_window_{self._default_recent_window}': {'default': 0, 'type': int, 'source_table': self.table_names['draws_flat']},
            'recurrence_cdf': {'default': 0.0, 'type': float, 'source_table': self.table_names['recurrence_cdf']},
            'rank_slope': {'default': 0.0, 'type': float, 'source_table': self.table_names['rank_trends']},
            'trend_status': {'default': 'indefinido', 'type': str, 'source_table': self.table_names['rank_trends']},
            'is_missing_in_current_cycle': {'default': 0, 'type': int, 'source_table': self.table_names['cycle_status']},
            'cycle_closing_propensity_score': {'default': 0.0, 'type': float, 'source_table': self.table_names['cycle_closing_propensity']},
            'participation_score_k2': {'default': 0.0, 'type': float, 'source_table': self.table_names['itemset_metrics']},
            'participation_score_k3': {'default': 0.0, 'type': float, 'source_table': self.table_names['itemset_metrics']},
        }
        self._target_metric_columns = list(self.metric_configs.keys())

    def _get_latest_concurso_id_from_db(self) -> Optional[int]:
        try:
            # Usa CONTEST_ID_COLUMN_NAME do config para consistência
            query = f"SELECT MAX({self.config_access.CONTEST_ID_COLUMN_NAME}) FROM {self.table_names['draws']}"
            result_df = self.db_manager.execute_query(query)
            if result_df is not None and not result_df.empty and pd.notna(result_df.iloc[0, 0]):
                return int(result_df.iloc[0, 0])
            logger.warning(f"Nenhum concurso encontrado na tabela '{self.table_names['draws']}'.")
        except Exception as e:
            logger.error(f"Falha ao buscar último concurso_id: {e}", exc_info=True)
        return None

    def get_historical_metrics_for_dezenas(self, latest_concurso_id: Optional[int] = None) -> pd.DataFrame:
        if latest_concurso_id is None:
            latest_concurso_id = self._get_latest_concurso_id_from_db()
            if latest_concurso_id is None:
                logger.error("Não foi possível determinar o concurso mais recente para agregação.")
                empty_df = pd.DataFrame({'dezena': self._all_dezenas_list})
                for col, conf in self.metric_configs.items(): empty_df[col] = conf['default']
                return empty_df.astype({col: conf['type'] for col, conf in self.metric_configs.items() if conf['type'] is not object and col in empty_df}, errors='ignore')

        logger.info(f"Consolidando métricas históricas para dezenas até o concurso {latest_concurso_id}...")
        dezenas_df = pd.DataFrame({'dezena': self._all_dezenas_list})

        merge_methods_map = {
            'delays': self._merge_delay_metrics,
            'frequency': self._merge_frequency_metrics,
            'recurrence': self._merge_recurrence_metrics,
            'rank_trends': self._merge_rank_trend_metrics,
            'cycle_status': self._merge_cycle_status_metrics,
            'itemset_participation': self._merge_itemset_participation_scores
        }

        for metric_group, merge_method in merge_methods_map.items():
            try:
                dezenas_df = merge_method(dezenas_df, latest_concurso_id)
            except Exception as e:
                logger.error(f"Erro durante {merge_method.__name__} para o grupo '{metric_group}': {e}", exc_info=True)
        
        for col_name, conf in self.metric_configs.items():
            if col_name not in dezenas_df.columns:
                dezenas_df[col_name] = conf['default']
            else:
                dezenas_df[col_name] = dezenas_df[col_name].fillna(conf['default'])
            try:
                 if conf['type'] is not object and col_name in dezenas_df.columns: # Adicionada checagem se col_name existe
                    dezenas_df[col_name] = dezenas_df[col_name].astype(conf['type'])
            except Exception as e:
                 logger.warning(f"Não foi possível converter a coluna '{col_name}' para o tipo {conf['type']}: {e}")

        final_cols_ordered = ['dezena'] + [col for col in self._target_metric_columns if col in dezenas_df.columns]
        other_cols = [col for col in dezenas_df.columns if col not in final_cols_ordered]
        dezenas_df = dezenas_df[final_cols_ordered + other_cols]
        
        return dezenas_df

    def _execute_metric_query(self, base_df: pd.DataFrame, sql: str, params: tuple,
                               expected_metric_cols: List[str], metric_group_name: str) -> pd.DataFrame:
        data_df = None
        try:
            data_df = self.db_manager.execute_query(sql, params)
        except Exception as e:
            logger.error(f"Falha na execução da query para {metric_group_name}: {e}", exc_info=True)

        if data_df is not None and not data_df.empty:
            if 'dezena' not in data_df.columns:
                logger.warning(f"Coluna 'dezena' ausente no resultado da query para {metric_group_name}.")
                for col in expected_metric_cols:
                    if col != 'dezena' and col not in base_df.columns: base_df[col] = self.metric_configs.get(col, {}).get('default', pd.NA)
                return base_df
            
            # Garantir que as colunas esperadas existam no data_df antes de selecionar
            actual_cols_from_query = ['dezena'] + [col for col in expected_metric_cols if col in data_df.columns and col != 'dezena']
            
            cols_to_drop_from_base = [col for col in actual_cols_from_query if col in base_df.columns and col != 'dezena']
            base_df_for_merge = base_df.drop(columns=cols_to_drop_from_base, errors='ignore')
            
            merged_df = pd.merge(base_df_for_merge, data_df[actual_cols_from_query], on='dezena', how='left')
            return merged_df
        else:
            logger.info(f"Nenhum dado encontrado via query para {metric_group_name} (params: {params}).")
            # Preencher com defaults se a query não retornar dados ou for vazia
            for col_metric in expected_metric_cols: # Iterar sobre expected_metric_cols, não apenas 'col'
                if col_metric != 'dezena' and col_metric not in base_df.columns:
                    base_df[col_metric] = self.metric_configs.get(col_metric, {}).get('default', pd.NA)
            return base_df

    def _merge_delay_metrics(self, base_df: pd.DataFrame, concurso_id: int) -> pd.DataFrame:
        table = self.table_names['delays']
        # Usando CONTEST_ID_COLUMN_NAME do config para consistência
        cid_col = self.config_access.CONTEST_ID_COLUMN_NAME
        sql = f"""
            WITH RankedData AS (
                SELECT dezena, current_delay,
                       COALESCE(max_delay_observed, current_delay) AS max_delay_observed,
                       COALESCE(avg_delay, current_delay) AS avg_delay,
                       ROW_NUMBER() OVER (PARTITION BY dezena ORDER BY {cid_col} DESC) as rn
                FROM {table} WHERE {cid_col} <= ?
            )
            SELECT dezena, current_delay, max_delay_observed, avg_delay FROM RankedData WHERE rn = 1;
        """
        params = (concurso_id,)
        metric_cols = ['current_delay', 'max_delay_observed', 'avg_delay']
        return self._execute_metric_query(base_df, sql, params, metric_cols, "métricas de atraso")

    def _merge_frequency_metrics(self, base_df: pd.DataFrame, concurso_id: int) -> pd.DataFrame:
        merged_df = base_df.copy()
        table_overall = self.table_names['frequency_overall']
        table_flat = self.table_names['draws_flat']
        cid_col = self.config_access.CONTEST_ID_COLUMN_NAME
        
        sql_overall = f"""
            WITH RankedData AS (
                SELECT dezena, frequency AS overall_frequency, relative_frequency AS overall_relative_frequency,
                       ROW_NUMBER() OVER (PARTITION BY dezena ORDER BY {cid_col} DESC) as rn
                FROM {table_overall} WHERE {cid_col} <= ?
            )
            SELECT dezena, overall_frequency, overall_relative_frequency FROM RankedData WHERE rn = 1;
        """
        params_overall = (concurso_id,)
        metric_cols_overall = ['overall_frequency', 'overall_relative_frequency']
        merged_df = self._execute_metric_query(merged_df, sql_overall, params_overall, metric_cols_overall, "frequência geral")

        window_size = self.config_access.AGGREGATOR_DEFAULT_RECENT_WINDOW # Usando a constante do config
        start_concurso_id = max(1, concurso_id - window_size + 1)
        col_name_recent_freq = f'recent_frequency_window_{window_size}'
        
        sql_recent = f"""
            SELECT
                gen_dez.value AS dezena, COUNT(drf.dezena) AS "{col_name_recent_freq}"
            FROM json_each(json_array({','.join(map(str, self._all_dezenas_list))})) gen_dez
            LEFT JOIN {table_flat} drf ON gen_dez.value = drf.dezena AND drf.{cid_col} BETWEEN ? AND ?
            GROUP BY gen_dez.value;
        """
        params_recent = (start_concurso_id, concurso_id)
        metric_cols_recent = [col_name_recent_freq]
        
        # Adicionar dinamicamente a configuração da métrica se a janela mudar e não existir
        if col_name_recent_freq not in self.metric_configs:
            self.metric_configs[col_name_recent_freq] = {'default': 0, 'type': int, 'source_table': table_flat}
            if col_name_recent_freq not in self._target_metric_columns: 
                self._target_metric_columns.append(col_name_recent_freq)
        
        return self._execute_metric_query(merged_df, sql_recent, params_recent, metric_cols_recent, f"frequência recente (W{window_size})")

    def _merge_recurrence_metrics(self, base_df: pd.DataFrame, concurso_id: int) -> pd.DataFrame:
        table = self.table_names['recurrence_cdf']
        cid_col = self.config_access.CONTEST_ID_COLUMN_NAME
        sql = f"""
            WITH RankedData AS (
                SELECT dezena, recurrence_cdf,
                       ROW_NUMBER() OVER (PARTITION BY dezena ORDER BY {cid_col} DESC) as rn
                FROM {table} WHERE {cid_col} <= ?
            )
            SELECT dezena, recurrence_cdf FROM RankedData WHERE rn = 1;
        """
        params = (concurso_id,)
        metric_cols = ['recurrence_cdf']
        return self._execute_metric_query(base_df, sql, params, metric_cols, "métricas de recorrência")

    def _merge_rank_trend_metrics(self, base_df: pd.DataFrame, concurso_id: int) -> pd.DataFrame:
        table = self.table_names['rank_trends']
        cid_col = self.config_access.CONTEST_ID_COLUMN_NAME
        sql = f"""
            WITH RankedData AS (
                SELECT dezena, rank_slope, trend_status,
                       ROW_NUMBER() OVER (PARTITION BY dezena ORDER BY {cid_col} DESC) as rn
                FROM {table} WHERE {cid_col} <= ?
            )
            SELECT dezena, rank_slope, trend_status FROM RankedData WHERE rn = 1;
        """
        params = (concurso_id,)
        metric_cols = ['rank_slope', 'trend_status']
        return self._execute_metric_query(base_df, sql, params, metric_cols, "tendência de rank")

    def _merge_cycle_status_metrics(self, base_df: pd.DataFrame, concurso_id: int) -> pd.DataFrame:
        table_status = self.table_names['cycle_status']
        table_closing = self.table_names['cycle_closing_propensity']
        cid_col = self.config_access.CONTEST_ID_COLUMN_NAME
        merged_df = base_df.copy()

        sql_missing = f"""
            WITH RankedData AS (
                SELECT dezena, is_missing_in_current_cycle,
                       ROW_NUMBER() OVER (PARTITION BY dezena ORDER BY {cid_col} DESC) as rn
                FROM {table_status} WHERE {cid_col} <= ?
            )
            SELECT dezena, is_missing_in_current_cycle FROM RankedData WHERE rn = 1;
        """
        params_missing = (concurso_id,)
        metric_cols_missing = ['is_missing_in_current_cycle']
        merged_df = self._execute_metric_query(merged_df, sql_missing, params_missing, metric_cols_missing, "status de ciclo (faltantes)")
        
        sql_closing_score = f"""SELECT dezena, score AS cycle_closing_propensity_score FROM {table_closing};"""
        params_closing = () 
        metric_cols_closing = ['cycle_closing_propensity_score']
        return self._execute_metric_query(merged_df, sql_closing_score, params_closing, metric_cols_closing, "propensão de fechamento de ciclo")

    def _calculate_dezena_itemset_scores(self, item_data_df: pd.DataFrame, k_value: int) -> pd.DataFrame:
        if item_data_df.empty or 'itemset' not in item_data_df.columns or 'itemset_score' not in item_data_df.columns:
            return pd.DataFrame({'dezena': self._all_dezenas_list, f'participation_score_k{k_value}': 0.0})

        filtered_items = item_data_df[item_data_df['k'] == k_value]
        if filtered_items.empty:
             return pd.DataFrame({'dezena': self._all_dezenas_list, f'participation_score_k{k_value}': 0.0})

        dezena_scores_acc: Dict[int, float] = {d: 0.0 for d in self._all_dezenas_list}
        for _, row in filtered_items.iterrows():
            itemset = row['itemset']
            item_score = row.get('itemset_score', 0.0) 
            if isinstance(itemset, tuple): # Checagem adicional de tipo
                for dezena_val in itemset: # Renomeado para evitar conflito com a coluna 'dezena'
                    if dezena_val in dezena_scores_acc:
                        dezena_scores_acc[dezena_val] += item_score
        
        scores_df = pd.DataFrame(list(dezena_scores_acc.items()), columns=['dezena', f'raw_participation_score_k{k_value}'])
        
        scaler = MinMaxScaler()
        score_col_raw = f'raw_participation_score_k{k_value}'
        score_col_norm = f'participation_score_k{k_value}'
        
        # Tratar caso onde a coluna pode não ter sido criada se dezena_scores_acc estava vazio ou todos os scores eram 0
        if score_col_raw not in scores_df.columns:
             scores_df[score_col_norm] = 0.0
        elif scores_df[score_col_raw].nunique() > 1:
            scores_df[score_col_norm] = scaler.fit_transform(scores_df[[score_col_raw]])
        else: # nunique <= 1
            scores_df[score_col_norm] = 0.5 if scores_df[score_col_raw].nunique() == 1 and scores_df[score_col_raw].iloc[0] !=0 else 0.0
        
        return scores_df[['dezena', score_col_norm]]


    def _merge_itemset_participation_scores(self, base_df: pd.DataFrame, concurso_id: int) -> pd.DataFrame:
        logger.info("Calculando scores de participação em itemsets...")
        merged_df = base_df.copy()
        k_values_to_process = self.config_access.config_obj.get('itemset_k_values_for_participation_score', [2,3])


        all_item_data = self.get_itemset_analysis_data(latest_concurso_id=concurso_id, k_values=k_values_to_process)

        if all_item_data is None or all_item_data.empty:
            logger.warning("Nenhum dado de itemset retornado pelo Aggregator para cálculo de scores de participação.")
            for k_val in k_values_to_process:
                 score_col_k = f'participation_score_k{k_val}'
                 if score_col_k not in merged_df.columns: merged_df[score_col_k] = self.metric_configs.get(score_col_k, {}).get('default', 0.0)
            return merged_df
        
        for k_val in k_values_to_process:
            score_col_k = f'participation_score_k{k_val}'
            itemset_scores_k_df = self._calculate_dezena_itemset_scores(all_item_data, k_val)
            if not itemset_scores_k_df.empty:
                merged_df = pd.merge(merged_df, itemset_scores_k_df, on='dezena', how='left')
            elif score_col_k not in merged_df.columns: # Garante a coluna se o df de scores k for vazio
                 merged_df[score_col_k] = self.metric_configs.get(score_col_k, {}).get('default', 0.0)
            
            # Preencher NaNs que podem ter surgido dos merges
            if score_col_k in merged_df.columns:
                merged_df[score_col_k] = merged_df[score_col_k].fillna(self.metric_configs.get(score_col_k, {}).get('default',0.0))
            else: # Se a coluna ainda não existe por algum motivo, cria com default
                merged_df[score_col_k] = self.metric_configs.get(score_col_k, {}).get('default', 0.0)

        return merged_df

    def get_itemset_analysis_data(self, latest_concurso_id: Optional[int] = None,
                                  k_values: Optional[List[int]] = None,
                                  min_support: Optional[float] = None,
                                  min_lift: Optional[float] = None,
                                  itemset_score_metric: str = 'itemset_score'
                                  ) -> pd.DataFrame:
        table_itemsets = self.table_names['itemset_metrics']
        # Busca de config_access ao invés de self.config.get
        default_k_from_config = getattr(self.config_access, 'ITEMSET_DEFAULT_K_VALUES_AGGREGATOR', [2, 3])


        if latest_concurso_id is None:
            latest_concurso_id = self._get_latest_concurso_id_from_db()
            if latest_concurso_id is None: return pd.DataFrame()

        where_clauses = [f"im.{self.config_access.CONTEST_ID_COLUMN_NAME} <= ?"] # Usando nome da coluna do config
        params_list: List[Any] = [latest_concurso_id]

        current_k_values = k_values if k_values is not None else default_k_from_config
        if current_k_values:
            safe_k_values = [int(k) for k in current_k_values if isinstance(k, (int, float)) or (isinstance(k, str) and k.isdigit())]
            if safe_k_values:
                 where_clauses.append(f"im.k IN ({','.join(['?'] * len(safe_k_values))})")
                 params_list.extend(safe_k_values)
        
        if min_support is not None:
            where_clauses.append("im.support >= ?")
            params_list.append(min_support)
        
        if min_lift is not None:
            where_clauses.append("im.lift >= ?")
            params_list.append(min_lift)
            
        where_sql = " AND ".join(where_clauses)
        
        allowed_score_cols = ['itemset_score', 'support', 'confidence', 'lift'] 
        score_col_to_select = itemset_score_metric if itemset_score_metric in allowed_score_cols else 'itemset_score'

        cid_col_for_query = self.config_access.CONTEST_ID_COLUMN_NAME # Usando nome da coluna do config
        sql = f"""
            WITH RankedItemsets AS (
                SELECT
                    itemset_str, k, support, COALESCE(lift, 0) AS lift, 
                    COALESCE({score_col_to_select}, 0) AS itemset_score,
                    COALESCE(itemset_current_delay, 0) AS itemset_current_delay,
                    ROW_NUMBER() OVER (PARTITION BY itemset_str, k ORDER BY {cid_col_for_query} DESC) as rn
                FROM {table_itemsets} im
                WHERE {where_sql}
            )
            SELECT itemset_str AS itemset_original_str, k, support, lift, itemset_score, itemset_current_delay
            FROM RankedItemsets WHERE rn = 1;
        """
        params = tuple(params_list)
        
        try:
            item_data_df = self.db_manager.execute_query(sql, params)
            if item_data_df is not None and not item_data_df.empty:
                def parse_itemset_str_local(itemset_string_val): # Renomeado para evitar conflito
                    try:
                        if isinstance(itemset_string_val, str):
                            cleaned_str = itemset_string_val.strip("() ")
                            if not cleaned_str: return tuple()
                            # Supondo que os números no itemset_str são separados por vírgula ou hífen
                            # Ex: "(1,5,10)" ou "1-5-10"
                            separator = ',' if ',' in cleaned_str else '-'
                            return tuple(map(int, cleaned_str.split(separator)))
                        elif isinstance(itemset_string_val, (list, set)):
                             return tuple(sorted(map(int,itemset_string_val)))
                        elif isinstance(itemset_string_val, tuple):
                             return tuple(sorted(map(int,itemset_string_val)))
                        return None
                    except Exception as e_parse:
                        logger.warning(f"Falha ao parsear itemset_str: '{itemset_string_val}'. Erro: {e_parse}")
                        return None
                item_data_df['itemset'] = item_data_df['itemset_original_str'].apply(parse_itemset_str_local)
                item_data_df.dropna(subset=['itemset'], inplace=True)
                return item_data_df[['itemset', 'k', 'support', 'lift', 'itemset_score', 'itemset_current_delay']]
            else:
                logger.info(f"Nenhum dado de itemset encontrado com os critérios (concurso {latest_concurso_id}).")
        except Exception as e:
            logger.error(f"Erro ao buscar dados de itemset: {e}", exc_info=True)
        
        return pd.DataFrame(columns=['itemset', 'k', 'support', 'lift', 'itemset_score', 'itemset_current_delay'])