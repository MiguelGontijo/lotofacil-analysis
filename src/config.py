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

DRAWN_NUMBERS_COLUMN_NAME: str = os.getenv('DRAWN_NUMBERS_COLUMN_NAME', 'drawn_numbers')
CONTEST_ID_COLUMN_NAME: str = os.getenv('CONTEST_ID_COLUMN_NAME', 'contest_id')
DATE_COLUMN_NAME: str = os.getenv('DATE_COLUMN_NAME', 'date')

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

# -----------------------------------------------------------------------------
# Configurações para Análise de Sequências Numéricas
# -----------------------------------------------------------------------------
SEQUENCE_ANALYSIS_CONFIG = {
    "consecutive": {          
        "min_len": 3,         
        "max_len": 5,         
        "active": True        
    },
    "arithmetic_steps": { 
        "steps_to_check": [2, 3],
        "min_len": 3,            
        "max_len": 4,            
        "active": True           
    }
}
# Nomes de Tabelas (agrupando por tema para melhor organização)
# Itemsets e Sequências
SEQUENCE_METRICS_TABLE_NAME = "sequence_metrics" 
FREQUENT_ITEMSETS_TABLE_NAME = "frequent_itemsets"
FREQUENT_ITEMSET_METRICS_TABLE_NAME = "frequent_itemset_metrics"
ASSOCIATION_RULES_TABLE_NAME: str = "association_rules"

# Análises Gerais de Dezenas/Sorteios
DRAW_POSITION_FREQUENCY_TABLE_NAME = "draw_position_frequency"
RECURRENCE_ANALYSIS_TABLE_NAME: str = "geral_recurrence_analysis"
GRID_LINE_DISTRIBUTION_TABLE_NAME: str = "grid_line_distribution"
GRID_COLUMN_DISTRIBUTION_TABLE_NAME: str = "grid_column_distribution"
STATISTICAL_TESTS_RESULTS_TABLE_NAME: str = "statistical_tests_results"

# Médias Móveis Gerais
GERAL_MA_FREQUENCY_TABLE_NAME: str = "geral_ma_frequency"
GERAL_MA_DELAY_TABLE_NAME: str = "geral_ma_delay"

# --- Configurações para Médias Móveis Gerais ---
_geral_ma_freq_windows_str: str = os.getenv('GERAL_MA_FREQ_WINDOWS', '5,10,20,30')
GERAL_MA_FREQUENCY_WINDOWS: List[int] = [int(w.strip()) for w in _geral_ma_freq_windows_str.split(',')]
_geral_ma_delay_windows_str: str = os.getenv('GERAL_MA_DELAY_WINDOWS', '5,10,20,30')
GERAL_MA_DELAY_WINDOWS: List[int] = [int(w.strip()) for w in _geral_ma_delay_windows_str.split(',')]

# --- Configurações para Regras de Associação (Market Basket Analysis) ---
ASSOCIATION_RULES_MIN_CONFIDENCE: float = float(os.getenv('ASSOCIATION_RULES_MIN_CONFIDENCE', '0.5'))
ASSOCIATION_RULES_MIN_LIFT: float = float(os.getenv('ASSOCIATION_RULES_MIN_LIFT', '1.0'))

# --- Configurações para Análise de Linhas e Colunas ---
LOTOFACIL_GRID_LINES: Dict[str, List[int]] = {
    "L1": [1, 2, 3, 4, 5], "L2": [6, 7, 8, 9, 10], "L3": [11, 12, 13, 14, 15],
    "L4": [16, 17, 18, 19, 20], "L5": [21, 22, 23, 24, 25],
}
LOTOFACIL_GRID_COLUMNS: Dict[str, List[int]] = {
    "C1": [1, 6, 11, 16, 21], "C2": [2, 7, 12, 17, 22], "C3": [3, 8, 13, 18, 23],
    "C4": [4, 9, 14, 19, 24], "C5": [5, 10, 15, 20, 25],
}

# --- Configurações para Testes Estatísticos ---
# Número de bins para o teste Qui-Quadrado de normalidade da soma das dezenas
SUM_NORMALITY_TEST_BINS: int = int(os.getenv('SUM_NORMALITY_TEST_BINS', '10'))

POISSON_DISTRIBUTION_TEST_CONFIG: Dict[str, Dict[str, Any]] = {
    "Count_Primos_Per_Draw": {
        "column_name": "primos", 
        "max_observed_count_for_chi2": 10 
    },
    "Count_Pares_Per_Draw": {
        "column_name": "pares",
        "max_observed_count_for_chi2": 10 
    },
    "Count_Impares_Per_Draw": {
        "column_name": "impares",
        "max_observed_count_for_chi2": 10
    }
}

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
    COLUMNS_TO_KEEP: List[str] = COLUMNS_TO_KEEP
    NEW_COLUMN_NAMES: List[str] = NEW_COLUMN_NAMES
    BALL_NUMBER_COLUMNS: List[str] = BALL_NUMBER_COLUMNS
    ALL_NUMBERS: List[int] = ALL_NUMBERS
    NUMBERS_PER_DRAW: int = NUMBERS_PER_DRAW
    DRAWN_NUMBERS_COLUMN_NAME: str = DRAWN_NUMBERS_COLUMN_NAME
    CONTEST_ID_COLUMN_NAME: str = CONTEST_ID_COLUMN_NAME
    DATE_COLUMN_NAME: str = DATE_COLUMN_NAME
    CHUNK_TYPES_CONFIG: Dict[str, List[int]] = CHUNK_TYPES_CONFIG
    CHUNK_TYPES: Dict[str, List[int]] = CHUNK_TYPES_CONFIG

    FREQUENCY_TOP_N_HOT: int = int(os.getenv('FREQUENCY_TOP_N_HOT', '5'))
    FREQUENCY_BOTTOM_N_COLD: int = int(os.getenv('FREQUENCY_BOTTOM_N_COLD', '5'))

    APRIORI_MIN_SUPPORT: float = APRIORI_MIN_SUPPORT
    FREQUENT_ITEMSETS_MIN_LEN: int = FREQUENT_ITEMSETS_MIN_LEN
    FREQUENT_ITEMSETS_MAX_LEN: int = FREQUENT_ITEMSETS_MAX_LEN
    
    LOG_LEVEL: str = LOG_LEVEL
    LOG_FILE: str = LOG_FILE

    DEFAULT_CHUNK_TYPE_FOR_PLOTTING: str = DEFAULT_CHUNK_TYPE_FOR_PLOTTING
    DEFAULT_CHUNK_SIZE_FOR_PLOTTING: int = DEFAULT_CHUNK_SIZE_FOR_PLOTTING
    DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT: List[int] = DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT
    
    SEQUENCE_ANALYSIS_CONFIG = SEQUENCE_ANALYSIS_CONFIG
    
    # Nomes de Tabelas como atributos da classe
    SEQUENCE_METRICS_TABLE_NAME = SEQUENCE_METRICS_TABLE_NAME
    FREQUENT_ITEMSETS_TABLE_NAME = FREQUENT_ITEMSETS_TABLE_NAME
    FREQUENT_ITEMSET_METRICS_TABLE_NAME = FREQUENT_ITEMSET_METRICS_TABLE_NAME
    DRAW_POSITION_FREQUENCY_TABLE_NAME = DRAW_POSITION_FREQUENCY_TABLE_NAME
    GERAL_MA_FREQUENCY_TABLE_NAME: str = GERAL_MA_FREQUENCY_TABLE_NAME
    GERAL_MA_DELAY_TABLE_NAME: str = GERAL_MA_DELAY_TABLE_NAME
    RECURRENCE_ANALYSIS_TABLE_NAME: str = RECURRENCE_ANALYSIS_TABLE_NAME
    ASSOCIATION_RULES_TABLE_NAME: str = ASSOCIATION_RULES_TABLE_NAME
    GRID_LINE_DISTRIBUTION_TABLE_NAME: str = GRID_LINE_DISTRIBUTION_TABLE_NAME
    GRID_COLUMN_DISTRIBUTION_TABLE_NAME: str = GRID_COLUMN_DISTRIBUTION_TABLE_NAME
    STATISTICAL_TESTS_RESULTS_TABLE_NAME: str = STATISTICAL_TESTS_RESULTS_TABLE_NAME

    GERAL_MA_FREQUENCY_WINDOWS: List[int] = GERAL_MA_FREQUENCY_WINDOWS
    GERAL_MA_DELAY_WINDOWS: List[int] = GERAL_MA_DELAY_WINDOWS
    
    ASSOCIATION_RULES_MIN_CONFIDENCE: float = ASSOCIATION_RULES_MIN_CONFIDENCE
    ASSOCIATION_RULES_MIN_LIFT: float = ASSOCIATION_RULES_MIN_LIFT
    
    LOTOFACIL_GRID_LINES: Dict[str, List[int]] = LOTOFACIL_GRID_LINES
    LOTOFACIL_GRID_COLUMNS: Dict[str, List[int]] = LOTOFACIL_GRID_COLUMNS
    
    # Nova constante para Testes Estatísticos
    SUM_NORMALITY_TEST_BINS: int = SUM_NORMALITY_TEST_BINS
    POISSON_DISTRIBUTION_TEST_CONFIG: Dict[str, Dict[str, Any]] = POISSON_DISTRIBUTION_TEST_CONFIG

    def __init__(self):
        os.makedirs(self.LOG_DIR, exist_ok=True)
        os.makedirs(self.PLOT_DIR, exist_ok=True)
        os.makedirs(self.DATA_DIR, exist_ok=True)
        logger.info("Objeto Config instanciado. Configurações carregadas.")
        logger.debug(f"BASE_DIR: {self.BASE_DIR}")
        logger.debug(f"DATA_DIR: {self.DATA_DIR}")
        # ... (outros logs de debug no __init__)

try:
    config_obj = Config()
except Exception as e:
    print(f"ERRO CRÍTICO: Falha ao instanciar Config global: {e}") 
    config_obj = None 

if __name__ == '__main__':
    if config_obj:
        print(f"BASE_DIR: {config_obj.BASE_DIR}")
        # ... (outros prints de teste)
        print(f"STATISTICAL_TESTS_RESULTS_TABLE_NAME: {config_obj.STATISTICAL_TESTS_RESULTS_TABLE_NAME}")
        print(f"SUM_NORMALITY_TEST_BINS: {config_obj.SUM_NORMALITY_TEST_BINS}") # Novo print
    else:
        print("Instância config_obj não pôde ser criada.")