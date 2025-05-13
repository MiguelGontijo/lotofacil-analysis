# src/config.py
from pathlib import Path

# --- Configurações de Diretório ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "Data"
LOG_DIR_CONFIG = BASE_DIR / "Logs"
PLOT_DIR_CONFIG = BASE_DIR / "Plots"

# --- Configurações de Arquivos de Dados ---
RAW_DATA_FILE_NAME = "historico.csv"
CLEANED_DATA_FILE_NAME = "historico_cleaned_numeros.pkl"
DB_FILE_NAME = "lotofacil_analysis.db"

# Constantes para data_loader.py
COLUMNS_TO_KEEP = [
    'Concurso', 'Data Sorteio', 'Bola1', 'Bola2', 'Bola3', 'Bola4', 'Bola5',
    'Bola6', 'Bola7', 'Bola8', 'Bola9', 'Bola10', 'Bola11', 'Bola12',
    'Bola13', 'Bola14', 'Bola15'
]
NEW_COLUMN_NAMES = [
    'Concurso', 'Data Sorteio',
    'bola_1', 'bola_2', 'bola_3', 'bola_4', 'bola_5',
    'bola_6', 'bola_7', 'bola_8', 'bola_9', 'bola_10', 'bola_11', 'bola_12',
    'bola_13', 'bola_14', 'bola_15'
]
BALL_NUMBER_COLUMNS = [f'bola_{i}' for i in range(1, 16)]

# --- Configurações do Jogo Lotofácil ---
ALL_NUMBERS = list(range(1, 26)) # <<<--- ALL_NUMBERS é uma lista
NUM_DEZENAS_LOTOFACIL = 15
MAX_NUMBER = 25

# --- Configurações de Análise de Chunks ---
CHUNK_TYPES_CONFIG = {
    "linear": {"sizes": [10, 25, 50, 75, 100, 150, 200], "description": "Blocos lineares de tamanhos fixos."},
    "fibonacci": {"sizes": [5, 8, 13, 21, 34, 55, 89, 144], "description": "Blocos com tamanhos baseados na sequência de Fibonacci."},
    "prime": {"sizes": [5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97], "description": "Blocos com tamanhos baseados em números primos."},
    "doidos": {"sizes": [33, 66, 99, 122, 157, 190], "description": "Blocos com tamanhos baseados em uma sequência arbitrária 'doida'."}
}

# --- Configurações Padrão para Plotagem (Exemplos) ---
DEFAULT_CHUNK_TYPE_FOR_PLOTTING = 'linear'
DEFAULT_CHUNK_SIZE_FOR_PLOTTING = 50
DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT = [7, 14, 21]