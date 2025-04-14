# src/config.py

from pathlib import Path
import logging
from typing import List, Dict # Adicionado List, Dict

# --- Definições Globais (Ex: Números) ---
# É melhor definir aqui para garantir consistência entre módulos
ALL_NUMBERS: List[int] = list(range(1, 26))
ALL_NUMBERS_SET: set = set(ALL_NUMBERS)

# --- Configurações de Caminhos ---
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'Data'
EXCEL_FILE_NAME = 'Lotofacil.xlsx'
EXCEL_FILE_PATH = DATA_DIR / EXCEL_FILE_NAME
DATABASE_NAME = 'lotofacil_data.db'
DATABASE_PATH = PROJECT_ROOT / DATABASE_NAME
PLOT_DIR = PROJECT_ROOT / 'plots'

# --- Configurações da Tabela do Banco de Dados ---
TABLE_NAME = 'sorteios' # Tabela principal
CYCLES_TABLE_NAME = 'ciclos'
FREQ_SNAP_TABLE_NAME = 'freq_geral_snap'

# --- Configurações de Colunas (Usadas principalmente pelo data_loader) ---
# Colunas originais a serem lidas do Excel
ORIGINAL_COLUMNS: List[str] = [
    'Concurso', 'Data Sorteio',
    'Bola1', 'Bola2', 'Bola3', 'Bola4', 'Bola5',
    'Bola6', 'Bola7', 'Bola8', 'Bola9', 'Bola10',
    'Bola11', 'Bola12', 'Bola13', 'Bola14', 'Bola15'
]
# Novo nome padrão para as colunas das bolas (b1 a b15)
NEW_BALL_COLUMNS: List[str] = [f'b{i}' for i in range(1, 16)]
# Mapeamento de nomes de colunas originais para novos nomes
COLUMN_MAPPING: Dict[str, str] = {
    'Concurso': 'concurso',
    'Data Sorteio': 'data_sorteio',
    **{f'Bola{i}': f'b{i}' for i in range(1, 16)} # Cria Bola1:b1, etc.
}
# Colunas que devem ser do tipo inteiro no DataFrame final
INT_COLUMNS: List[str] = ['concurso'] + NEW_BALL_COLUMNS
# Coluna(s) que devem ser do tipo data/hora
DATE_COLUMNS: List[str] = ['data_sorteio']

# --- Configurações de Análise (Padrões) ---
DEFAULT_WINDOWS: List[int] = [10, 25, 50, 100, 200]
DEFAULT_SNAPSHOT_INTERVALS: List[int] = [10, 25, 50, 100, 200, 300, 400, 500]

# --- Configurações de Logging ---
LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)
logger = logging.getLogger('LotofacilAnalysis')

# --- Verificações Iniciais (Opcional) ---
# Podemos remover ou comentar se não for estritamente necessário no início
# if not EXCEL_FILE_PATH.is_file():
#     logger.critical(f"Arquivo de dados CRÍTICO não encontrado: {EXCEL_FILE_PATH}")
#     # Em um cenário real, talvez levantar um erro aqui para parar tudo.
#     # raise FileNotFoundError(f"Arquivo de dados não encontrado: {EXCEL_FILE_PATH}")