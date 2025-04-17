# src/config.py

from pathlib import Path
import logging
from typing import List, Dict, Set

# --- Definições Globais ---
ALL_NUMBERS: List[int] = list(range(1, 26))
ALL_NUMBERS_SET: Set[int] = set(ALL_NUMBERS)

# --- Configurações de Caminhos ---
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'Data'
EXCEL_FILE_NAME = 'Lotofacil.xlsx'
EXCEL_FILE_PATH = DATA_DIR / EXCEL_FILE_NAME
DATABASE_NAME = 'lotofacil_data.db'
DATABASE_PATH = PROJECT_ROOT / DATABASE_NAME
PLOT_DIR = PROJECT_ROOT / 'plots'

# --- Configurações de Tabelas BD ---
TABLE_NAME = 'sorteios'
CYCLES_TABLE_NAME = 'ciclos'
FREQ_SNAP_TABLE_NAME = 'freq_geral_snap'
CHUNK_STATS_FINAL_PREFIX = 'chunk_stats_'

# --- Configurações de Colunas (Data Loader) ---
ORIGINAL_COLUMNS: List[str] = ['Concurso','Data Sorteio'] + [f'Bola{i}' for i in range(1, 16)]
NEW_BALL_COLUMNS: List[str] = [f'b{i}' for i in range(1, 16)]
COLUMN_MAPPING: Dict[str, str] = {'Concurso': 'concurso', 'Data Sorteio': 'data_sorteio', **{f'Bola{i}': f'b{i}' for i in range(1, 16)}}
INT_COLUMNS: List[str] = ['concurso'] + NEW_BALL_COLUMNS
DATE_COLUMNS: List[str] = ['data_sorteio']
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS

# --- Configurações de Análise (Padrões) ---
# Janelas/Intervalos
AGGREGATOR_WINDOWS: List[int] = [10, 25, 50, 100, 200, 300, 400, 500]
DEFAULT_CMD_WINDOWS: str = '10,25,50'
DEFAULT_SNAPSHOT_INTERVALS: List[int] = [10, 25, 50, 100, 200, 300, 400, 500]
TREND_SHORT_WINDOW = 10; TREND_LONG_WINDOW = 50
DEFAULT_RANK_TREND_LOOKBACK: int = 50
# <<< CONSTANTE ADICIONADA >>>
DEFAULT_GROUP_WINDOWS: List[int] = [25, 100] # Janelas padrão para análise de grupo
# Chunk Intervals
STANDARD_CHUNK_INTERVALS = [10, 25, 50, 100, 200, 300, 400, 500]
FIBONACCI_CHUNK_INTERVALS = [5, 8, 13, 21, 34, 55, 89]
ALL_CHUNK_INTERVALS = sorted(list(set(STANDARD_CHUNK_INTERVALS + FIBONACCI_CHUNK_INTERVALS)))

# --- Configurações de Logging ---
LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)
logger = logging.getLogger('LotofacilAnalysis')