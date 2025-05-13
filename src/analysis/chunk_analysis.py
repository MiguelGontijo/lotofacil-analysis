# src/analysis/chunk_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any, Set 
import logging
import numpy as np # Para np.nan e cálculos de média

from src.config import ALL_NUMBERS, CHUNK_TYPES_CONFIG
from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)
ALL_NUMBERS_AS_SET: Set[int] = set(ALL_NUMBERS)

def get_chunk_definitions(total_contests: int, chunk_config_type: str, chunk_config_sizes: List[int]) -> List[Tuple[int, int, str]]:
    definitions: List[Tuple[int, int, str]] = []
    if not chunk_config_sizes:
        logger.warning(f"Nenhum tamanho configurado para o tipo de bloco: {chunk_config_type}")
        return definitions
    for sz_item in chunk_config_sizes:
        if sz_item <= 0:
            logger.warning(f"Tamanho de chunk inválido: {sz_item} para o tipo {chunk_config_type}. Pulando.")
            continue
        current_pos = 0
        while current_pos < total_contests:
            start_contest = current_pos + 1
            end_contest = min(current_pos + sz_item, total_contests)
            suffix = f"{chunk_config_type}_{sz_item}"
            definitions.append((start_contest, end_contest, suffix))
            current_pos = end_contest
            if current_pos >= total_contests:
                break
    logger.debug(f"Definições de chunk para tipo='{chunk_config_type}', tamanhos={chunk_config_sizes}: {len(definitions)} blocos gerados.")
    return definitions

def calculate_frequency_in_chunk(df_chunk: pd.DataFrame) -> pd.Series:
    if df_chunk.empty:
        return pd.Series(0, index=pd.Index(ALL_NUMBERS, name="dezena"), name="frequencia_absoluta", dtype='int')
    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
    if not actual_dezena_cols:
        return pd.Series(0, index=pd.Index(ALL_NUMBERS, name="dezena"), name="frequencia_absoluta", dtype='int')
    all_drawn_numbers_in_chunk = df_chunk[actual_dezena_cols].values.flatten()
    frequency_series = pd.Series(all_drawn_numbers_in_chunk).value_counts()
    frequency_series = frequency_series.reindex(ALL_NUMBERS, fill_value=0)
    frequency_series.name = "frequencia_absoluta"
    frequency_series.index.name = "dezena"
    return frequency_series.astype(int)

def calculate_mean_delay_in_chunk(df_chunk: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int) -> pd.Series:
    """
    Calcula o atraso médio de cada dezena DENTRO de um chunk específico.
    O atraso é contado a partir do início do chunk ou da última aparição dentro do chunk.
    """
    mean_delays_in_chunk = pd.Series(np.nan, index=pd.Index(ALL_NUMBERS, name="dezena"), name="atraso_medio_no_bloco", dtype='float')

    if df_chunk.empty:
        # Se o chunk está vazio, todas as dezenas têm um "atraso" igual ao tamanho do chunk (ou NaN se preferir)
        # Para consistência com "nunca saiu no chunk", vamos usar o tamanho do chunk.
        chunk_duration = chunk_end_contest - chunk_start_contest + 1
        mean_delays_in_chunk = mean_delays_in_chunk.fillna(float(chunk_duration))
        return mean_delays_in_chunk

    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
    if not actual_dezena_cols:
        chunk_duration = chunk_end_contest - chunk_start_contest + 1
        mean_delays_in_chunk = mean_delays_in_chunk.fillna(float(chunk_duration))
        return mean_delays_in_chunk

    # Ordenar os concursos dentro do chunk para processamento sequencial de atraso
    df_chunk_sorted = df_chunk.sort_values(by='Concurso')

    for dezena_val in ALL_NUMBERS:
        gaps_for_dezena: List[int] = []
        last_occurrence_contest = chunk_start_contest - 1 # Início virtual antes do chunk

        for index, row in df_chunk_sorted.iterrows():
            current_contest_in_chunk = int(row['Concurso'])
            drawn_numbers_in_contest = set(int(num) for num in row[actual_dezena_cols].dropna().values)

            if dezena_val in drawn_numbers_in_contest:
                gap = current_contest_in_chunk - last_occurrence_contest - 1
                gaps_for_dezena.append(gap)
                last_occurrence_contest = current_contest_in_chunk
        
        # Adiciona o gap final (da última ocorrência até o final do chunk)
        final_gap = chunk_end_contest - last_occurrence_contest
        gaps_for_dezena.append(final_gap)

        if gaps_for_dezena:
            mean_delays_in_chunk.loc[dezena_val] = np.mean(gaps_for_dezena)
        else: # Dezena não apareceu no chunk (improvável se gaps_for_dezena sempre tem final_gap)
              # Se nunca apareceu, last_occurrence_contest = chunk_start_contest - 1
              # final_gap = chunk_end_contest - (chunk_start_contest - 1) = chunk_duration
              # gaps_for_dezena seria [chunk_duration], então a média é chunk_duration.
            mean_delays_in_chunk.loc[dezena_val] = float(chunk_end_contest - chunk_start_contest + 1)
            
    return mean_delays_in_chunk

def calculate_chunk_metrics_and_persist(all_data_df: pd.DataFrame, db_manager: DatabaseManager):
    logger.info("Iniciando cálculo e persistência de métricas de chunk (Frequência e Atraso Médio).") # Log Atualizado
    if 'Concurso' not in all_data_df.columns:
        logger.error("Coluna 'Concurso' não encontrada. Não é possível processar chunks.")
        return
        
    total_contests = all_data_df['Concurso'].max()
    if pd.isna(total_contests) or total_contests <= 0:
        logger.error(f"Número total de concursos inválido: {total_contests}. Não é possível processar chunks.")
        return

    for chunk_type, config in CHUNK_TYPES_CONFIG.items():
        chunk_sizes = config.get('sizes', [])
        for size in chunk_sizes:
            logger.info(f"Processando chunks: tipo='{chunk_type}', tamanho={size} para Frequência e Atraso Médio.")
            chunk_definitions = get_chunk_definitions(total_contests, chunk_type, [size])
            if not chunk_definitions:
                logger.warning(f"Nenhuma definição de chunk para tipo='{chunk_type}', tamanho={size}. Pulando.")
                continue

            all_frequency_metrics: List[Dict[str, Any]] = []
            all_mean_delay_metrics: List[Dict[str, Any]] = []
            
            for idx, (start_contest, end_contest, _) in enumerate(chunk_definitions):
                mask = (all_data_df['Concurso'] >= start_contest) & (all_data_df['Concurso'] <= end_contest)
                df_current_chunk = all_data_df[mask]

                # Calcular Frequência
                frequency_series_chunk = calculate_frequency_in_chunk(df_current_chunk)
                for dezena_val, freq_val in frequency_series_chunk.items():
                    all_frequency_metrics.append({
                        'chunk_seq_id': idx + 1, 
                        'chunk_start_contest': start_contest,
                        'chunk_end_contest': end_contest,
                        'dezena': int(dezena_val),
                        'frequencia_absoluta': int(freq_val)
                    })
                
                # Calcular Atraso Médio no Bloco
                mean_delay_series_chunk = calculate_mean_delay_in_chunk(df_current_chunk, start_contest, end_contest)
                for dezena_val, delay_val in mean_delay_series_chunk.items():
                    all_mean_delay_metrics.append({
                        'chunk_seq_id': idx + 1,
                        'chunk_start_contest': start_contest,
                        'chunk_end_contest': end_contest,
                        'dezena': int(dezena_val),
                        'atraso_medio_no_bloco': float(delay_val) if pd.notna(delay_val) else None # Garante float ou None
                    })

            # Salvar métricas de frequência
            if all_frequency_metrics:
                freq_metrics_df = pd.DataFrame(all_frequency_metrics)
                freq_table_name = f"evol_metric_frequency_{chunk_type}_{size}"
                try:
                    db_manager.save_dataframe_to_db(freq_metrics_df, freq_table_name, if_exists='replace')
                    logger.info(f"Métricas de frequência para '{chunk_type}_{size}' salvas em '{freq_table_name}'. {len(freq_metrics_df)} registros.")
                except Exception as e:
                    logger.error(f"Erro ao salvar frequência para '{chunk_type}_{size}': {e}", exc_info=True)
            
            # Salvar métricas de atraso médio no bloco
            if all_mean_delay_metrics:
                delay_metrics_df = pd.DataFrame(all_mean_delay_metrics)
                delay_table_name = f"evol_metric_atraso_medio_bloco_{chunk_type}_{size}" # Novo nome de tabela
                try:
                    db_manager.save_dataframe_to_db(delay_metrics_df, delay_table_name, if_exists='replace')
                    logger.info(f"Métricas de atraso médio no bloco para '{chunk_type}_{size}' salvas em '{delay_table_name}'. {len(delay_metrics_df)} registros.")
                except Exception as e:
                    logger.error(f"Erro ao salvar atraso médio no bloco para '{chunk_type}_{size}': {e}", exc_info=True)

    logger.info("Cálculo e persistência de métricas de chunk (Frequência e Atraso Médio) concluídos.")