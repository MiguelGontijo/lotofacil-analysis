# src/analysis/frequency_analysis.py

import pandas as pd
from typing import Optional, List

# Importa do config
from src.config import logger, NEW_BALL_COLUMNS, ALL_NUMBERS
# Importa funções do DB Manager
from src.database_manager import read_data_from_db, get_closest_freq_snapshot

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


# --- FUNÇÃO PARA CÁLCULO EM RANGES (NÃO USA SNAPSHOT) ---
def calculate_frequency(concurso_minimo: Optional[int] = None,
                        concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """ Calcula a frequência para um período específico (min/max). """
    period_str = f"[{concurso_minimo or 'início'} - {concurso_maximo or 'fim'}]"
    logger.info(f"Calculando frequência (direta) no período {period_str}...")
    df = read_data_from_db(columns=BASE_COLS, concurso_minimo=concurso_minimo, concurso_maximo=concurso_maximo)
    if df is None: return None # Erro na leitura
    if df.empty: logger.warning(f"Nenhum dado {period_str}."); return pd.Series(0, index=ALL_NUMBERS) # Retorna zeros se vazio

    melted_balls = df[NEW_BALL_COLUMNS].melt(value_name='number')['number'].dropna().astype(int)
    frequency = melted_balls.value_counts()
    frequency = frequency.reindex(ALL_NUMBERS, fill_value=0) # Usa ALL_NUMBERS
    frequency.sort_index(inplace=True)
    logger.info(f"Cálculo de frequência (direta) concluído.")
    return frequency


# --- FUNÇÃO PARA FREQUÊNCIA CUMULATIVA USANDO SNAPSHOTS ---
def get_cumulative_frequency(concurso_maximo: int) -> Optional[pd.Series]:
    """ Obtém a frequência geral acumulada até um concurso_maximo, usando snapshots. """
    if concurso_maximo <= 0: logger.error("Concurso máximo inválido."); return None
    logger.info(f"Obtendo frequência acumulada até {concurso_maximo} (snapshots)...")
    snapshot_info = get_closest_freq_snapshot(concurso_maximo)
    start_contest_delta = 1
    base_freq = pd.Series(0, index=ALL_NUMBERS) # Usa ALL_NUMBERS

    if snapshot_info:
        snap_concurso, snap_freq = snapshot_info
        # Validação extra do snapshot lido
        if snap_freq is None or len(snap_freq) != 25 or snap_freq.isnull().any():
             logger.warning(f"Snapshot inválido lido para {snap_concurso}. Recalculando do início.")
             start_contest_delta = 1 # Volta pro início
             base_freq = pd.Series(0, index=ALL_NUMBERS) # Zera base
        else:
             logger.debug(f"Usando snapshot do concurso {snap_concurso}.")
             base_freq = snap_freq.copy().astype(int) # Garante cópia e tipo
             start_contest_delta = snap_concurso + 1
    else:
         logger.info("Nenhum snapshot encontrado, calculando frequência total do início...")
         # Se não tem snapshot, calcula tudo direto (pode ser lento)
         # Alternativa: retornar erro ou None? Vamos calcular por enquanto.
         # return calculate_frequency(concurso_maximo=concurso_maximo) # Chama a função que NÃO usa snapshot

    # Verifica se precisa calcular delta
    if start_contest_delta > concurso_maximo:
        logger.debug("Snapshot já está no ponto. Retornando frequência base.")
        return base_freq.reindex(ALL_NUMBERS, fill_value=0).astype(int)
    else:
        logger.debug(f"Calculando delta de frequência de {start_contest_delta} a {concurso_maximo}...")
        # Calcula a frequência apenas para o período delta
        delta_freq = calculate_frequency(concurso_minimo=start_contest_delta, concurso_maximo=concurso_maximo)

        if delta_freq is None:
             logger.error(f"Falha ao calcular delta freq ({start_contest_delta}-{concurso_maximo}). Retornando None.")
             return None

        # Soma a frequência base (do snapshot) com a frequência delta
        cumulative_freq = base_freq.add(delta_freq, fill_value=0).reindex(ALL_NUMBERS, fill_value=0).astype(int) # Usa ALL_NUMBERS
        logger.info(f"Frequência acumulada até {concurso_maximo} obtida com sucesso.")
        return cumulative_freq


# --- Funções de Janela e Histórico Completo (Mantidas como antes) ---
def calculate_windowed_frequency(window_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    # (Código idêntico ao da última versão)
    logger.info(f"Calculando freq. janela {window_size} até {concurso_maximo or 'último'}...")
    df_all = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo);
    if df_all is None or df_all.empty: return None
    actual_max_c_val = df_all['concurso'].max();
    if pd.isna(actual_max_c_val): return None
    actual_max_c = int(actual_max_c_val)
    effective_max_c = min(int(concurso_maximo), actual_max_c) if concurso_maximo else actual_max_c
    min_c_win = effective_max_c - window_size + 1
    df_window = df_all[df_all['concurso'] >= min_c_win].copy()
    if df_window.empty: return pd.Series(0, index=ALL_NUMBERS)
    melted = df_window[NEW_BALL_COLUMNS].melt(value_name='number')['number'].dropna().astype(int)
    freq = melted.value_counts().reindex(ALL_NUMBERS, fill_value=0).sort_index()
    logger.info(f"Freq. janela {window_size} concluída."); return freq

def calculate_cumulative_frequency_history(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    # (Código idêntico ao da última versão)
    logger.info(f"Calculando histórico acumulado até {concurso_maximo or 'último'}...")
    df = read_data_from_db(columns=['concurso','data_sorteio']+NEW_BALL_COLUMNS, concurso_maximo=concurso_maximo);
    if df is None or df.empty: return None
    melted = df.melt(id_vars=['concurso'], value_vars=NEW_BALL_COLUMNS, value_name='number'); melted = melted[['concurso', 'number']].dropna(); melted['number'] = melted['number'].astype(int)
    counts_pivot = pd.pivot_table(melted, index='concurso', columns='number', aggfunc='size', fill_value=0); counts_pivot = counts_pivot.reindex(columns=ALL_NUMBERS, fill_value=0)
    cumulative_freq = counts_pivot.cumsum(axis=0); cumulative_freq.columns = [f'cum_freq_{i}' for i in ALL_NUMBERS]
    if 'data_sorteio' in df.columns: df_dates = df[['concurso', 'data_sorteio']].drop_duplicates('concurso').set_index('concurso'); cumulative_freq = df_dates.join(cumulative_freq, how='right'); cumulative_freq.reset_index(inplace=True)
    logger.info("Histórico acumulado concluído."); return cumulative_freq