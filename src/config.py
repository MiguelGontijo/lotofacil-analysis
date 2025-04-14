# src/config.py

from pathlib import Path
import logging

# --- Configurações de Caminhos ---
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'Data'
EXCEL_FILE_NAME = 'Lotofacil.xlsx'
EXCEL_FILE_PATH = DATA_DIR / EXCEL_FILE_NAME
DATABASE_NAME = 'lotofacil_data.db'
DATABASE_PATH = PROJECT_ROOT / DATABASE_NAME

# --- Configurações da Tabela do Banco de Dados ---
TABLE_NAME = 'sorteios' # Nome da tabela onde os dados serão armazenados

# --- Configurações de Colunas ---
ORIGINAL_COLUMNS = [
    'Concurso', 'Data Sorteio',
    'Bola1', 'Bola2', 'Bola3', 'Bola4', 'Bola5',
    'Bola6', 'Bola7', 'Bola8', 'Bola9', 'Bola10',
    'Bola11', 'Bola12', 'Bola13', 'Bola14', 'Bola15'
]
NEW_BALL_COLUMNS = [f'b{i}' for i in range(1, 16)]
COLUMN_MAPPING = {
    'Concurso': 'concurso',
    'Data Sorteio': 'data_sorteio',
    **{f'Bola{i}': f'b{i}' for i in range(1, 16)}
}
INT_COLUMNS = ['concurso'] + NEW_BALL_COLUMNS
DATE_COLUMNS = ['data_sorteio']

# --- Configurações de Logging ---
LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)

# --- Verificações Iniciais ---
if not EXCEL_FILE_PATH.is_file():
    logger.error(f"Arquivo de dados não encontrado em: {EXCEL_FILE_PATH}")
    # Considerar levantar uma exceção FileNotFoundError aqui para parar a execução