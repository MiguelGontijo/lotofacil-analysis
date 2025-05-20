# Lotofacil_Analysis/src/config.py
import os
import logging
from dotenv import load_dotenv
from typing import List, Dict, Any

load_dotenv()
logger = logging.getLogger(__name__)

# --- Definições de Constantes no Nível do Módulo ---
BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR: str = os.path.join(BASE_DIR, 'Data')
LOG_DIR: str = os.path.join(BASE_DIR, 'Logs')
PLOT_DIR: str = os.path.join(BASE_DIR, 'Plots')

PLOT_DIR_CONFIG: str = os.getenv('PLOT_DIR', PLOT_DIR)
RAW_DATA_FILE_NAME: str = os.getenv('RAW_DATA_FILE_NAME', 'historico.csv')
CLEANED_DATA_FILE_NAME: str = os.getenv('CLEANED_DATA_FILE_NAME', 'cleaned_draws.pkl')

_columns_to_keep_str: str = os.getenv('COLUMNS_TO_KEEP', 'Concurso,Data Sorteio,Bola1,Bola2,Bola3,Bola4,Bola5,Bola6,Bola7,Bola8,Bola9,Bola10,Bola11,Bola12,Bola13,Bola14,Bola15')
COLUMNS_TO_KEEP: List[str] = [col.strip() for col in _columns_to_keep_str.split(',')]

_new_column_names_str: str = os.getenv('NEW_COLUMN_NAMES', 'contest_id,Data Sorteio,ball_1,ball_2,ball_3,ball_4,ball_5,ball_6,ball_7,ball_8,ball_9,ball_10,ball_11,ball_12,ball_13,ball_14,ball_15')
NEW_COLUMN_NAMES: List[str] = [col.strip() for col in _new_column_names_str.split(',')]

_ball_number_columns_str: str = os.getenv('BALL_NUMBER_COLUMNS', 'ball_1,ball_2,ball_3,ball_4,ball_5,ball_6,ball_7,ball_8,ball_9,ball_10,ball_11,ball_12,ball_13,ball_14,ball_15')
BALL_NUMBER_COLUMNS: List[str] = [col.strip() for col in _ball_number_columns_str.split(',')]

DB_NAME: str = os.getenv('DB_NAME', 'lotofacil.db')
DB_PATH: str = os.path.join(DATA_DIR, DB_NAME)

HISTORICO_CSV_FILENAME: str = RAW_DATA_FILE_NAME
HISTORICO_CSV_PATH: str = os.path.join(DATA_DIR, HISTORICO_CSV_FILENAME)
CLEANED_DATA_PATH: str = os.path.join(DATA_DIR, CLEANED_DATA_FILE_NAME)

ALL_NUMBERS: List[int] = list(range(1, 26))
NUMBERS_PER_DRAW: int = 15

# --- Nomes de Colunas Padrão (Chaves Primárias e Identificadores) ---
DRAWN_NUMBERS_COLUMN_NAME: str = os.getenv('DRAWN_NUMBERS_COLUMN_NAME', 'drawn_numbers')
CONTEST_ID_COLUMN_NAME: str = os.getenv('CONTEST_ID_COLUMN_NAME', 'contest_id')
DATE_COLUMN_NAME: str = os.getenv('DATE_COLUMN_NAME', 'date')
DEZENA_COLUMN_NAME: str = "dezena"

# --- Nomes de Colunas para Tabelas de Análise Específicas (Já Adicionados) ---
CURRENT_DELAY_COLUMN_NAME: str = "current_delay"
MAX_DELAY_OBSERVED_COLUMN_NAME: str = "max_delay_observed"
AVG_DELAY_COLUMN_NAME: str = "avg_delay"
FREQUENCY_COLUMN_NAME: str = "frequency"
RELATIVE_FREQUENCY_COLUMN_NAME: str = "relative_frequency"
RECURRENCE_CDF_COLUMN_NAME: str = "recurrence_cdf"
RANK_SLOPE_COLUMN_NAME: str = "rank_slope"
TREND_STATUS_COLUMN_NAME: str = "trend_status"
CHUNK_TYPE_COLUMN_NAME: str = "chunk_type"
CHUNK_SIZE_COLUMN_NAME: str = "chunk_size"
CICLO_NUM_COLUMN_NAME: str = "ciclo_num"
IS_MISSING_IN_CURRENT_CYCLE_COLUMN_NAME: str = "is_missing_in_current_cycle"
CYCLE_CLOSING_SCORE_COLUMN_NAME: str = "score" # Coluna na DB, aggregator renomeia para cycle_closing_propensity_score
ITEMSET_STR_COLUMN_NAME: str = "itemset_str"
K_COLUMN_NAME: str = "k"
SUPPORT_COLUMN_NAME: str = "support"
CONFIDENCE_COLUMN_NAME: str = "confidence"
LIFT_COLUMN_NAME: str = "lift"
ITEMSET_SCORE_COLUMN_NAME: str = "itemset_score"
ITEMSET_CURRENT_DELAY_COLUMN_NAME: str = "itemset_current_delay"
ITEMSET_AVG_DELAY_COLUMN_NAME: str = "itemset_avg_delay"
ITEMSET_MAX_DELAY_COLUMN_NAME: str = "itemset_max_delay"

# --- Nomes de Tabelas Padronizados ---
MAIN_DRAWS_TABLE_NAME: str = "draws"
FLAT_DRAWS_TABLE_NAME: str = "draw_results_flat"

# Tabelas de Análise Primárias (consumidas pelo AnalysisAggregator)
ANALYSIS_DELAYS_TABLE_NAME: str = "analysis_delays"
ANALYSIS_FREQUENCY_OVERALL_TABLE_NAME: str = "analysis_frequency_overall"
ANALYSIS_RECURRENCE_CDF_TABLE_NAME: str = "analysis_recurrence_cdf"
ANALYSIS_ITEMSET_METRICS_TABLE_NAME: str = "analysis_itemset_metrics"
ANALYSIS_CYCLE_STATUS_DEZENAS_TABLE_NAME: str = "analysis_cycle_status_dezenas"
ANALYSIS_CYCLE_CLOSING_PROPENSITY_TABLE_NAME: str = "analysis_cycle_closing_propensity"
ANALYSIS_RANK_TREND_METRICS_TABLE_NAME: str = "analysis_rank_trend_metrics"

# Outras Tabelas de Análise e Base
PROPRIEDADES_NUMERICAS_POR_CONCURSO_TABLE_NAME: str = "propriedades_numericas_por_concurso"
REPETICAO_CONCURSO_ANTERIOR_TABLE_NAME: str = "analise_repeticao_concurso_anterior"
CHUNK_METRICS_TABLE_NAME: str = "chunk_metrics"
DRAW_POSITION_FREQUENCY_TABLE_NAME: str = "draw_position_frequency"
GERAL_MA_FREQUENCY_TABLE_NAME: str = "geral_ma_frequency"
GERAL_MA_DELAY_TABLE_NAME: str = "geral_ma_delay"
ASSOCIATION_RULES_TABLE_NAME: str = "association_rules"
GRID_LINE_DISTRIBUTION_TABLE_NAME: str = "grid_line_distribution"
GRID_COLUMN_DISTRIBUTION_TABLE_NAME: str = "grid_column_distribution"
STATISTICAL_TESTS_RESULTS_TABLE_NAME: str = "statistical_tests_results"
MONTHLY_NUMBER_FREQUENCY_TABLE_NAME: str = "monthly_number_frequency"
MONTHLY_DRAW_PROPERTIES_TABLE_NAME: str = "monthly_draw_properties_summary"
SEQUENCE_METRICS_TABLE_NAME: str = "sequence_metrics"
FREQUENT_ITEMSETS_TABLE_NAME: str = "frequent_itemsets" # Tabela base para itemset_metrics

# Tabelas de Ciclo (Detalhes, Sumário, Progressão Bruta, Métricas por Ciclo)
ANALYSIS_CYCLES_DETAIL_TABLE_NAME: str = "analysis_cycles_detail"
ANALYSIS_CYCLES_SUMMARY_TABLE_NAME: str = "analysis_cycles_summary"
ANALYSIS_CYCLE_PROGRESSION_RAW_TABLE_NAME: str = "analysis_cycle_progression_raw"
CYCLES_DETAIL_TABLE_NAME_INPUT_FOR_AGG: str = ANALYSIS_CYCLES_DETAIL_TABLE_NAME # Para BlockAggregator
CYCLE_METRIC_FREQUENCY_TABLE_NAME: str = "ciclo_metric_frequency"
CYCLE_METRIC_ATRASO_MEDIO_TABLE_NAME: str = "ciclo_metric_atraso_medio"
CYCLE_METRIC_ATRASO_MAXIMO_TABLE_NAME: str = "ciclo_metric_atraso_maximo"
CYCLE_METRIC_ATRASO_FINAL_TABLE_NAME: str = "ciclo_metric_atraso_final"
CYCLE_RANK_FREQUENCY_TABLE_NAME: str = "ciclo_rank_frequency"
CYCLE_GROUP_METRICS_TABLE_NAME: str = "ciclo_group_metrics"

# Prefixos e Nomes de Tabelas Agregadas
EVOL_METRIC_FREQUENCY_BLOCK_PREFIX: str = "evol_metric_frequency_bloco"
EVOL_RANK_FREQUENCY_BLOCK_PREFIX: str = "evol_rank_frequency_bloco"
EVOL_METRIC_ATRASO_MEDIO_BLOCK_PREFIX: str = "evol_metric_atraso_medio_bloco"
EVOL_METRIC_ATRASO_MAXIMO_BLOCK_PREFIX: str = "evol_metric_atraso_maximo_bloco"
EVOL_METRIC_ATRASO_FINAL_BLOCK_PREFIX: str = "evol_metric_atraso_final_bloco"
EVOL_METRIC_OCCURRENCE_STD_DEV_BLOCK_PREFIX: str = "evol_metric_occurrence_std_dev_bloco"
EVOL_METRIC_DELAY_STD_DEV_BLOCK_PREFIX: str = "evol_metric_delay_std_dev_bloco"
EVOL_BLOCK_GROUP_METRICS_PREFIX: str = "evol_block_group_metrics"

BLOCK_ANALISES_CONSOLIDADAS_PREFIX: str = "bloco_analises_consolidadas"
CYCLE_ANALISES_CONSOLIDADAS_TABLE_NAME: str = "cycle_analises_consolidadas"

_default_chunk_type_for_rank_trend_env: str = os.getenv('DEFAULT_CHUNK_TYPE_FOR_PLOTTING', 'linear')
_default_chunk_size_for_rank_trend_env: str = os.getenv('DEFAULT_CHUNK_SIZE_FOR_PLOTTING', '50')
BLOCK_AGGREGATED_DATA_FOR_RANK_TREND_TABLE_NAME: str = f"{BLOCK_ANALISES_CONSOLIDADAS_PREFIX}_{_default_chunk_type_for_rank_trend_env}_{_default_chunk_size_for_rank_trend_env}"


# --- Outras Configurações de Análise ---
# (Restante das suas configurações como CHUNK_TYPES_CONFIG, APRIORI, LOG_LEVEL, etc. permanecem aqui)
# ... (COLE O RESTANTE DO SEU ARQUIVO CONFIG.PY ORIGINAL A PARTIR DAQUI) ...

CHUNK_TYPES_CONFIG: Dict[str, List[int]] = {
    "linear": [int(s.strip()) for s in os.getenv('CHUNK_TYPES_LINEAR', '10,25,50,75,100,150,200').split(',')],
    "fibonacci": [int(s.strip()) for s in os.getenv('CHUNK_TYPES_FIBONACCI', '5,8,13,21,34,55,89,144').split(',')],
    "primes": [int(s.strip()) for s in os.getenv('CHUNK_TYPES_PRIMES', '2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97').split(',')],
    "doidos": [int(s.strip()) for s in os.getenv('CHUNK_TYPES_DOIDOS', '33,66,99,122,157,190').split(',')]
}
APRIORI_MIN_SUPPORT: float = float(os.getenv('APRIORI_MIN_SUPPORT', 0.02))
FREQUENT_ITEMSETS_MIN_LEN: int = int(os.getenv('FREQUENT_ITEMSETS_MIN_LEN', 3))
FREQUENT_ITEMSETS_MAX_LEN: int = int(os.getenv('FREQUENT_ITEMSETS_MAX_LEN', 8))
LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_FILE: str = os.path.join(LOG_DIR, 'lotofacil_analysis.log')
DEFAULT_CHUNK_TYPE_FOR_PLOTTING: str = os.getenv('DEFAULT_CHUNK_TYPE_FOR_PLOTTING', 'linear')
DEFAULT_CHUNK_SIZE_FOR_PLOTTING: int = int(os.getenv('DEFAULT_CHUNK_SIZE_FOR_PLOTTING', '50'))
_default_dezenas_plot_str: str = os.getenv('DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT', '1,7,13,19,25')
DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT: List[int] = [int(d.strip()) for d in _default_dezenas_plot_str.split(',')]
SEQUENCE_ANALYSIS_CONFIG = {
    "consecutive": {"min_len": 3, "max_len": 5, "active": True},
    "arithmetic_steps": {"steps_to_check": [2, 3], "min_len": 3, "max_len": 4, "active": True}
}
_geral_ma_freq_windows_str: str = os.getenv('GERAL_MA_FREQ_WINDOWS', '5,10,20,30')
GERAL_MA_FREQUENCY_WINDOWS: List[int] = [int(w.strip()) for w in _geral_ma_freq_windows_str.split(',')]
_geral_ma_delay_windows_str: str = os.getenv('GERAL_MA_DELAY_WINDOWS', '5,10,20,30')
GERAL_MA_DELAY_WINDOWS: List[int] = [int(w.strip()) for w in _geral_ma_delay_windows_str.split(',')]
ASSOCIATION_RULES_MIN_CONFIDENCE: float = float(os.getenv('ASSOCIATION_RULES_MIN_CONFIDENCE', '0.5'))
ASSOCIATION_RULES_MIN_LIFT: float = float(os.getenv('ASSOCIATION_RULES_MIN_LIFT', '1.0'))
LOTOFACIL_GRID_LINES: Dict[str, List[int]] = {
    "L1": [1, 2, 3, 4, 5], "L2": [6, 7, 8, 9, 10], "L3": [11, 12, 13, 14, 15],
    "L4": [16, 17, 18, 19, 20], "L5": [21, 22, 23, 24, 25],
}
LOTOFACIL_GRID_COLUMNS: Dict[str, List[int]] = {
    "C1": [1, 6, 11, 16, 21], "C2": [2, 7, 12, 17, 22], "C3": [3, 8, 13, 18, 23],
    "C4": [4, 9, 14, 19, 24], "C5": [5, 10, 15, 20, 25],
}
SUM_NORMALITY_TEST_BINS: int = int(os.getenv('SUM_NORMALITY_TEST_BINS', '10'))
POISSON_DISTRIBUTION_TEST_CONFIG: Dict[str, Dict[str, Any]] = {
    "Count_Primos_Per_Draw": {"column_name": "primos", "max_observed_count_for_chi2": 10},
    "Count_Pares_Per_Draw": {"column_name": "pares", "max_observed_count_for_chi2": 10},
    "Count_Impares_Per_Draw": {"column_name": "impares", "max_observed_count_for_chi2": 10}
}
AGGREGATOR_DEFAULT_RECENT_WINDOW: int = int(os.getenv('AGGREGATOR_DEFAULT_RECENT_WINDOW', '10'))
MIN_CONTESTS_FOR_HISTORICAL_DELAY: int = int(os.getenv('MIN_CONTESTS_FOR_HISTORICAL_DELAY', '10'))
MIN_CONTESTS_FOR_HISTORICAL_RECURRENCE: int = int(os.getenv('MIN_CONTESTS_FOR_HISTORICAL_RECURRENCE', '10'))
MIN_CONTESTS_FOR_ITEMSET_METRICS: int = int(os.getenv('MIN_CONTESTS_FOR_ITEMSET_METRICS', '10'))
_itemset_k_part_score_str = os.getenv('ITEMSET_K_VALUES_FOR_PARTICIPATION_SCORE', '2,3')
ITEMSET_K_VALUES_FOR_PARTICIPATION_SCORE: List[int] = [int(k.strip()) for k in _itemset_k_part_score_str.split(',')]
_itemset_default_k_agg_str = os.getenv('ITEMSET_DEFAULT_K_VALUES_AGGREGATOR', '2,3')
ITEMSET_DEFAULT_K_VALUES_AGGREGATOR: List[int] = [int(k.strip()) for k in _itemset_default_k_agg_str.split(',')]
RANK_TREND_WINDOW_BLOCKS: int = int(os.getenv('RANK_TREND_WINDOW_BLOCKS', '5'))
RANK_TREND_SLOPE_IMPROVING_THRESHOLD: float = float(os.getenv('RANK_TREND_SLOPE_IMPROVING_THRESHOLD', '-0.1'))
RANK_TREND_SLOPE_WORSENING_THRESHOLD: float = float(os.getenv('RANK_TREND_SLOPE_WORSENING_THRESHOLD', '0.1'))
RANK_VALUE_COLUMN_FOR_TREND: str = os.getenv('RANK_VALUE_COLUMN_FOR_TREND', 'rank_no_bloco')
RANK_ANALYSIS_TYPE_FILTER_FOR_TREND: str = os.getenv('RANK_ANALYSIS_TYPE_FILTER_FOR_TREND', 'rank_freq_bloco')


class Config:
    BASE_DIR: str = BASE_DIR
    DATA_DIR: str = DATA_DIR
    LOG_DIR: str = LOG_DIR
    PLOT_DIR: str = PLOT_DIR
    PLOT_DIR_CONFIG: str = PLOT_DIR_CONFIG
    DB_PATH: str = DB_PATH
    RAW_DATA_FILE_NAME: str = RAW_DATA_FILE_NAME
    CLEANED_DATA_FILE_NAME: str = CLEANED_DATA_FILE_NAME
    HISTORICO_CSV_PATH: str = HISTORICO_CSV_PATH
    CLEANED_DATA_PATH: str = CLEANED_DATA_PATH

    ALL_NUMBERS: List[int] = ALL_NUMBERS
    NUMBERS_PER_DRAW: int = NUMBERS_PER_DRAW

    COLUMNS_TO_KEEP: List[str] = COLUMNS_TO_KEEP
    NEW_COLUMN_NAMES: List[str] = NEW_COLUMN_NAMES
    BALL_NUMBER_COLUMNS: List[str] = BALL_NUMBER_COLUMNS
    DRAWN_NUMBERS_COLUMN_NAME: str = DRAWN_NUMBERS_COLUMN_NAME
    CONTEST_ID_COLUMN_NAME: str = CONTEST_ID_COLUMN_NAME
    DATE_COLUMN_NAME: str = DATE_COLUMN_NAME
    DEZENA_COLUMN_NAME: str = DEZENA_COLUMN_NAME

    CURRENT_DELAY_COLUMN_NAME: str = CURRENT_DELAY_COLUMN_NAME
    MAX_DELAY_OBSERVED_COLUMN_NAME: str = MAX_DELAY_OBSERVED_COLUMN_NAME
    AVG_DELAY_COLUMN_NAME: str = AVG_DELAY_COLUMN_NAME
    FREQUENCY_COLUMN_NAME: str = FREQUENCY_COLUMN_NAME
    RELATIVE_FREQUENCY_COLUMN_NAME: str = RELATIVE_FREQUENCY_COLUMN_NAME
    RECURRENCE_CDF_COLUMN_NAME: str = RECURRENCE_CDF_COLUMN_NAME
    RANK_SLOPE_COLUMN_NAME: str = RANK_SLOPE_COLUMN_NAME
    TREND_STATUS_COLUMN_NAME: str = TREND_STATUS_COLUMN_NAME
    CHUNK_TYPE_COLUMN_NAME: str = CHUNK_TYPE_COLUMN_NAME
    CHUNK_SIZE_COLUMN_NAME: str = CHUNK_SIZE_COLUMN_NAME
    CICLO_NUM_COLUMN_NAME: str = CICLO_NUM_COLUMN_NAME
    IS_MISSING_IN_CURRENT_CYCLE_COLUMN_NAME: str = IS_MISSING_IN_CURRENT_CYCLE_COLUMN_NAME
    CYCLE_CLOSING_SCORE_COLUMN_NAME: str = CYCLE_CLOSING_SCORE_COLUMN_NAME
    ITEMSET_STR_COLUMN_NAME: str = ITEMSET_STR_COLUMN_NAME
    K_COLUMN_NAME: str = K_COLUMN_NAME
    SUPPORT_COLUMN_NAME: str = SUPPORT_COLUMN_NAME
    CONFIDENCE_COLUMN_NAME: str = CONFIDENCE_COLUMN_NAME
    LIFT_COLUMN_NAME: str = LIFT_COLUMN_NAME
    ITEMSET_SCORE_COLUMN_NAME: str = ITEMSET_SCORE_COLUMN_NAME
    ITEMSET_CURRENT_DELAY_COLUMN_NAME: str = ITEMSET_CURRENT_DELAY_COLUMN_NAME
    ITEMSET_AVG_DELAY_COLUMN_NAME: str = ITEMSET_AVG_DELAY_COLUMN_NAME
    ITEMSET_MAX_DELAY_COLUMN_NAME: str = ITEMSET_MAX_DELAY_COLUMN_NAME

    MAIN_DRAWS_TABLE_NAME: str = MAIN_DRAWS_TABLE_NAME
    FLAT_DRAWS_TABLE_NAME: str = FLAT_DRAWS_TABLE_NAME
    ANALYSIS_DELAYS_TABLE_NAME: str = ANALYSIS_DELAYS_TABLE_NAME
    ANALYSIS_FREQUENCY_OVERALL_TABLE_NAME: str = ANALYSIS_FREQUENCY_OVERALL_TABLE_NAME
    ANALYSIS_RECURRENCE_CDF_TABLE_NAME: str = ANALYSIS_RECURRENCE_CDF_TABLE_NAME
    ANALYSIS_ITEMSET_METRICS_TABLE_NAME: str = ANALYSIS_ITEMSET_METRICS_TABLE_NAME
    ANALYSIS_CYCLE_STATUS_DEZENAS_TABLE_NAME: str = ANALYSIS_CYCLE_STATUS_DEZENAS_TABLE_NAME
    ANALYSIS_CYCLE_CLOSING_PROPENSITY_TABLE_NAME: str = ANALYSIS_CYCLE_CLOSING_PROPENSITY_TABLE_NAME
    ANALYSIS_RANK_TREND_METRICS_TABLE_NAME: str = ANALYSIS_RANK_TREND_METRICS_TABLE_NAME
    ANALYSIS_CYCLES_DETAIL_TABLE_NAME: str = ANALYSIS_CYCLES_DETAIL_TABLE_NAME
    ANALYSIS_CYCLES_SUMMARY_TABLE_NAME: str = ANALYSIS_CYCLES_SUMMARY_TABLE_NAME
    ANALYSIS_CYCLE_PROGRESSION_RAW_TABLE_NAME: str = ANALYSIS_CYCLE_PROGRESSION_RAW_TABLE_NAME
    
    PROPRIEDADES_NUMERICAS_POR_CONCURSO_TABLE_NAME: str = PROPRIEDADES_NUMERICAS_POR_CONCURSO_TABLE_NAME
    REPETICAO_CONCURSO_ANTERIOR_TABLE_NAME: str = REPETICAO_CONCURSO_ANTERIOR_TABLE_NAME
    CHUNK_METRICS_TABLE_NAME: str = CHUNK_METRICS_TABLE_NAME
    DRAW_POSITION_FREQUENCY_TABLE_NAME: str = DRAW_POSITION_FREQUENCY_TABLE_NAME
    GERAL_MA_FREQUENCY_TABLE_NAME: str = GERAL_MA_FREQUENCY_TABLE_NAME
    GERAL_MA_DELAY_TABLE_NAME: str = GERAL_MA_DELAY_TABLE_NAME
    ASSOCIATION_RULES_TABLE_NAME: str = ASSOCIATION_RULES_TABLE_NAME
    GRID_LINE_DISTRIBUTION_TABLE_NAME: str = GRID_LINE_DISTRIBUTION_TABLE_NAME
    GRID_COLUMN_DISTRIBUTION_TABLE_NAME: str = GRID_COLUMN_DISTRIBUTION_TABLE_NAME
    STATISTICAL_TESTS_RESULTS_TABLE_NAME: str = STATISTICAL_TESTS_RESULTS_TABLE_NAME
    MONTHLY_NUMBER_FREQUENCY_TABLE_NAME: str = MONTHLY_NUMBER_FREQUENCY_TABLE_NAME
    MONTHLY_DRAW_PROPERTIES_TABLE_NAME: str = MONTHLY_DRAW_PROPERTIES_TABLE_NAME
    SEQUENCE_METRICS_TABLE_NAME: str = SEQUENCE_METRICS_TABLE_NAME
    FREQUENT_ITEMSETS_TABLE_NAME: str = FREQUENT_ITEMSETS_TABLE_NAME

    EVOL_METRIC_FREQUENCY_BLOCK_PREFIX: str = EVOL_METRIC_FREQUENCY_BLOCK_PREFIX
    EVOL_RANK_FREQUENCY_BLOCK_PREFIX: str = EVOL_RANK_FREQUENCY_BLOCK_PREFIX
    EVOL_METRIC_ATRASO_MEDIO_BLOCK_PREFIX: str = EVOL_METRIC_ATRASO_MEDIO_BLOCK_PREFIX
    EVOL_METRIC_ATRASO_MAXIMO_BLOCK_PREFIX: str = EVOL_METRIC_ATRASO_MAXIMO_BLOCK_PREFIX
    EVOL_METRIC_ATRASO_FINAL_BLOCK_PREFIX: str = EVOL_METRIC_ATRASO_FINAL_BLOCK_PREFIX
    EVOL_METRIC_OCCURRENCE_STD_DEV_BLOCK_PREFIX: str = EVOL_METRIC_OCCURRENCE_STD_DEV_BLOCK_PREFIX
    EVOL_METRIC_DELAY_STD_DEV_BLOCK_PREFIX: str = EVOL_METRIC_DELAY_STD_DEV_BLOCK_PREFIX
    EVOL_BLOCK_GROUP_METRICS_PREFIX: str = EVOL_BLOCK_GROUP_METRICS_PREFIX

    CYCLES_DETAIL_TABLE_NAME_INPUT_FOR_AGG: str = CYCLES_DETAIL_TABLE_NAME_INPUT_FOR_AGG
    CYCLE_METRIC_FREQUENCY_TABLE_NAME: str = CYCLE_METRIC_FREQUENCY_TABLE_NAME
    CYCLE_METRIC_ATRASO_MEDIO_TABLE_NAME: str = CYCLE_METRIC_ATRASO_MEDIO_TABLE_NAME
    CYCLE_METRIC_ATRASO_MAXIMO_TABLE_NAME: str = CYCLE_METRIC_ATRASO_MAXIMO_TABLE_NAME
    CYCLE_METRIC_ATRASO_FINAL_TABLE_NAME: str = CYCLE_METRIC_ATRASO_FINAL_TABLE_NAME
    CYCLE_RANK_FREQUENCY_TABLE_NAME: str = CYCLE_RANK_FREQUENCY_TABLE_NAME
    CYCLE_GROUP_METRICS_TABLE_NAME: str = CYCLE_GROUP_METRICS_TABLE_NAME

    BLOCK_ANALISES_CONSOLIDADAS_PREFIX: str = BLOCK_ANALISES_CONSOLIDADAS_PREFIX
    BLOCK_AGGREGATED_DATA_FOR_RANK_TREND_TABLE_NAME: str = BLOCK_AGGREGATED_DATA_FOR_RANK_TREND_TABLE_NAME
    CYCLE_ANALISES_CONSOLIDADAS_TABLE_NAME: str = CYCLE_ANALISES_CONSOLIDADAS_TABLE_NAME

    CHUNK_TYPES_CONFIG: Dict[str, List[int]] = CHUNK_TYPES_CONFIG
    CHUNK_TYPES: Dict[str, List[int]] = CHUNK_TYPES_CONFIG

    APRIORI_MIN_SUPPORT: float = APRIORI_MIN_SUPPORT
    FREQUENT_ITEMSETS_MIN_LEN: int = FREQUENT_ITEMSETS_MIN_LEN
    FREQUENT_ITEMSETS_MAX_LEN: int = FREQUENT_ITEMSETS_MAX_LEN

    LOG_LEVEL: str = LOG_LEVEL
    LOG_FILE: str = LOG_FILE

    DEFAULT_CHUNK_TYPE_FOR_PLOTTING: str = DEFAULT_CHUNK_TYPE_FOR_PLOTTING
    DEFAULT_CHUNK_SIZE_FOR_PLOTTING: int = DEFAULT_CHUNK_SIZE_FOR_PLOTTING
    DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT: List[int] = DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT

    SEQUENCE_ANALYSIS_CONFIG: Dict[str,Dict[str,Any]] = SEQUENCE_ANALYSIS_CONFIG
    GERAL_MA_FREQUENCY_WINDOWS: List[int] = GERAL_MA_FREQUENCY_WINDOWS
    GERAL_MA_DELAY_WINDOWS: List[int] = GERAL_MA_DELAY_WINDOWS
    ASSOCIATION_RULES_MIN_CONFIDENCE: float = ASSOCIATION_RULES_MIN_CONFIDENCE
    ASSOCIATION_RULES_MIN_LIFT: float = ASSOCIATION_RULES_MIN_LIFT
    LOTOFACIL_GRID_LINES: Dict[str, List[int]] = LOTOFACIL_GRID_LINES
    LOTOFACIL_GRID_COLUMNS: Dict[str, List[int]] = LOTOFACIL_GRID_COLUMNS
    SUM_NORMALITY_TEST_BINS: int = SUM_NORMALITY_TEST_BINS
    POISSON_DISTRIBUTION_TEST_CONFIG: Dict[str, Dict[str, Any]] = POISSON_DISTRIBUTION_TEST_CONFIG

    AGGREGATOR_DEFAULT_RECENT_WINDOW: int = AGGREGATOR_DEFAULT_RECENT_WINDOW
    MIN_CONTESTS_FOR_HISTORICAL_DELAY: int = MIN_CONTESTS_FOR_HISTORICAL_DELAY
    MIN_CONTESTS_FOR_HISTORICAL_RECURRENCE: int = MIN_CONTESTS_FOR_HISTORICAL_RECURRENCE
    MIN_CONTESTS_FOR_ITEMSET_METRICS: int = MIN_CONTESTS_FOR_ITEMSET_METRICS
    ITEMSET_K_VALUES_FOR_PARTICIPATION_SCORE: List[int] = ITEMSET_K_VALUES_FOR_PARTICIPATION_SCORE
    ITEMSET_DEFAULT_K_VALUES_AGGREGATOR: List[int] = ITEMSET_DEFAULT_K_VALUES_AGGREGATOR

    RANK_TREND_WINDOW_BLOCKS: int = RANK_TREND_WINDOW_BLOCKS
    RANK_TREND_SLOPE_IMPROVING_THRESHOLD: float = RANK_TREND_SLOPE_IMPROVING_THRESHOLD
    RANK_TREND_SLOPE_WORSENING_THRESHOLD: float = RANK_TREND_SLOPE_WORSENING_THRESHOLD
    RANK_VALUE_COLUMN_FOR_TREND: str = RANK_VALUE_COLUMN_FOR_TREND
    RANK_ANALYSIS_TYPE_FILTER_FOR_TREND: str = RANK_ANALYSIS_TYPE_FILTER_FOR_TREND

    def __init__(self):
        os.makedirs(self.LOG_DIR, exist_ok=True)
        os.makedirs(self.PLOT_DIR, exist_ok=True)
        os.makedirs(self.DATA_DIR, exist_ok=True)
        logger.info("Objeto Config instanciado. Configurações carregadas.")
        # Log de debug para verificar constantes chave
        key_debug_attrs = [
            'CONTEST_ID_COLUMN_NAME', 'DEZENA_COLUMN_NAME', 'ANALYSIS_DELAYS_TABLE_NAME',
            'CURRENT_DELAY_COLUMN_NAME', 'FREQUENCY_COLUMN_NAME', 'RECURRENCE_CDF_COLUMN_NAME',
            'RANK_SLOPE_COLUMN_NAME', 'CICLO_NUM_COLUMN_NAME', 'CYCLE_CLOSING_SCORE_COLUMN_NAME',
            'ITEMSET_STR_COLUMN_NAME', 'ANALYSIS_ITEMSET_METRICS_TABLE_NAME',
            'PROPRIEDADES_NUMERICAS_POR_CONCURSO_TABLE_NAME', 
            'REPETICAO_CONCURSO_ANTERIOR_TABLE_NAME', 'CHUNK_METRICS_TABLE_NAME'
        ]
        for attr_name in key_debug_attrs:
            if hasattr(self, attr_name):
                logger.debug(f"{attr_name}: {getattr(self, attr_name)}")

try:
    config_obj = Config()
except Exception as e:
    print(f"ERRO CRÍTICO: Falha ao instanciar Config global: {e}")
    logger.critical(f"ERRO CRÍTICO: Falha ao instanciar Config global: {e}", exc_info=True)
    config_obj = None # type: ignore

if __name__ == '__main__':
    if config_obj:
        print("--- Testando Constantes de Configuração ---")
        for attr_name in dir(config_obj):
            if not attr_name.startswith('__') and not callable(getattr(config_obj, attr_name)):
                print(f"{attr_name}: {getattr(config_obj, attr_name)}")
    else:
        print("Instância config_obj não pôde ser criada.")