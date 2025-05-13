# src/analysis/chunk_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any, Set
import logging
import numpy as np

from src.config import ALL_NUMBERS, CHUNK_TYPES_CONFIG
from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)
ALL_NUMBERS_AS_SET: Set[int] = set(ALL_NUMBERS)

def get_chunk_definitions(total_contests: int, chunk_config_type: str, chunk_config_sizes: List[int]) -> List[Tuple[int, int, str]]:
    # ... (código mantido como antes, já é eficiente) ...
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
    # ... (código mantido como antes, já é relativamente eficiente) ...
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

# --- Funções de Atraso Otimizadas ---

def get_draw_matrix_for_chunk(df_chunk: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int) -> pd.DataFrame:
    """
    Cria uma matriz de presença (1/0) para dezenas em concursos de um chunk.
    Linhas são concursos, colunas são dezenas. Preserva a ordem original dos concursos no chunk.
    """
    chunk_duration = chunk_end_contest - chunk_start_contest + 1
    if df_chunk.empty:
        # Retorna uma matriz de zeros com todos os concursos do chunk se o df_chunk for vazio
        # Isso pode não ser o ideal se não houver concursos.
        # Se df_chunk é vazio porque não há concursos nesse range, está ok.
        # Se é porque o slice resultou em vazio, mas os concursos existem, então 0 é correto.
        # Para simplificar, se vazio, assumimos que nada ocorreu.
        logger.debug(f"Chunk C{chunk_start_contest}-C{chunk_end_contest} vazio para get_draw_matrix_for_chunk.")
        return pd.DataFrame(0, index=pd.RangeIndex(start=chunk_start_contest, stop=chunk_end_contest + 1, name='Concurso'), columns=ALL_NUMBERS)


    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]

    if not actual_dezena_cols: # Sem colunas de bola, não podemos determinar presença
        logger.warning(f"Nenhuma coluna de bola em df_chunk para C{chunk_start_contest}-C{chunk_end_contest}.")
        return pd.DataFrame(0, index=pd.RangeIndex(start=chunk_start_contest, stop=chunk_end_contest + 1, name='Concurso'), columns=ALL_NUMBERS)

    # Cria um MultiIndex para cada dezena em cada concurso
    melted_df = df_chunk.melt(id_vars=['Concurso'], value_vars=actual_dezena_cols, value_name='Dezena')
    melted_df.dropna(subset=['Dezena'], inplace=True) # Remove NaNs se alguma bola não foi preenchida
    melted_df['Dezena'] = melted_df['Dezena'].astype(int)
    melted_df['presente'] = 1
    
    # Pivot para criar a matriz de presença
    # Usa todos os concursos no range do chunk, mesmo que não haja dados para eles (preenche com 0)
    all_contests_in_chunk_range = pd.Index(range(chunk_start_contest, chunk_end_contest + 1), name='Concurso')
    
    try:
        draw_matrix = melted_df.pivot_table(index='Concurso', columns='Dezena', values='presente', fill_value=0)
        # Reindex para garantir todas as dezenas e todos os concursos no range do chunk
        draw_matrix = draw_matrix.reindex(index=all_contests_in_chunk_range, columns=ALL_NUMBERS, fill_value=0)
    except Exception as e:
        logger.error(f"Erro ao pivotar dados para draw_matrix no chunk C{chunk_start_contest}-C{chunk_end_contest}: {e}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=ALL_NUMBERS) # Fallback

    return draw_matrix.astype(int)


def calculate_delays_for_matrix(draw_matrix: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int) -> Dict[str, pd.Series]:
    """
    Calcula Atraso Final, Atraso Médio e Atraso Máximo para cada dezena
    a partir de uma matriz de presença (concursos x dezenas).
    """
    chunk_duration = chunk_end_contest - chunk_start_contest + 1
    results = {
        "final": pd.Series(chunk_duration, index=ALL_NUMBERS, dtype='int'),
        "mean": pd.Series(float(chunk_duration), index=ALL_NUMBERS, dtype='float'), # Default se nunca saiu
        "max": pd.Series(chunk_duration, index=ALL_NUMBERS, dtype='int')
    }

    if draw_matrix.empty:
        logger.debug(f"Matriz de sorteios vazia para C{chunk_start_contest}-C{chunk_end_contest}. Atrasos serão duração do chunk.")
        return results

    for dezena in ALL_NUMBERS: # Itera sobre as dezenas (colunas da matriz)
        if dezena not in draw_matrix.columns: # Segurança, embora draw_matrix deva ter todas
            logger.warning(f"Dezena {dezena} não encontrada na draw_matrix. Usando defaults de atraso.")
            continue

        col_dezena = draw_matrix[dezena] # Série de 0s e 1s para a dezena, indexada por Concurso
        
        # Encontra os índices (números dos concursos) onde a dezena apareceu
        occurrence_contests = col_dezena[col_dezena == 1].index.to_list()

        if not occurrence_contests: # Dezena não apareceu no chunk
            # Atraso final, médio e máximo são a duração do chunk
            results["final"].loc[dezena] = chunk_duration
            results["mean"].loc[dezena] = float(chunk_duration)
            results["max"].loc[dezena] = chunk_duration
            continue

        # Atraso Final no Bloco
        results["final"].loc[dezena] = chunk_end_contest - occurrence_contests[-1]

        # Cálculo de Gaps para Atraso Médio e Máximo
        gaps = []
        # Gap inicial: da (partida_chunk - 1) até a primeira ocorrência
        gaps.append(occurrence_contests[0] - (chunk_start_contest -1) - 1)
        # Gaps entre ocorrências
        for i in range(len(occurrence_contests) - 1):
            gaps.append(occurrence_contests[i+1] - occurrence_contests[i] - 1)
        # Gap final: da última ocorrência até o final do chunk
        gaps.append(chunk_end_contest - occurrence_contests[-1])
        
        if gaps:
            results["mean"].loc[dezena] = np.mean(gaps)
            results["max"].loc[dezena] = max(gaps)
        else: # Não deveria acontecer se occurrence_contests não for vazio
            results["mean"].loc[dezena] = float(chunk_duration)
            results["max"].loc[dezena] = chunk_duration
            
    results["mean"].name = "atraso_medio_no_bloco"
    results["max"].name = "atraso_maximo_no_bloco"
    results["final"].name = "atraso_final_no_bloco"
    
    return results


def calculate_chunk_metrics_and_persist(all_data_df: pd.DataFrame, db_manager: DatabaseManager):
    logger.info("Iniciando cálculo de métricas de chunk (Frequência e Atrasos Otimizados).")
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
            logger.info(f"Processando chunks: tipo='{chunk_type}', tamanho={size} para todas as métricas.")
            chunk_definitions = get_chunk_definitions(total_contests, chunk_type, [size])
            if not chunk_definitions:
                logger.warning(f"Nenhuma definição de chunk para tipo='{chunk_type}', tamanho={size}. Pulando.")
                continue

            # Listas para armazenar os dados de todas as métricas antes de converter para DataFrame
            all_metrics_data: List[Dict[str, Any]] = []
            
            for idx, (start_contest, end_contest, _) in enumerate(chunk_definitions):
                mask = (all_data_df['Concurso'] >= start_contest) & (all_data_df['Concurso'] <= end_contest)
                df_current_chunk = all_data_df[mask]

                # Frequência
                frequency_series = calculate_frequency_in_chunk(df_current_chunk)
                
                # Atrasos (usando a matriz de presença otimizada)
                draw_matrix_chunk = get_draw_matrix_for_chunk(df_current_chunk, start_contest, end_contest)
                delay_metrics_dict = calculate_delays_for_matrix(draw_matrix_chunk, start_contest, end_contest)
                
                mean_delay_series = delay_metrics_dict["mean"]
                max_delay_series = delay_metrics_dict["max"]
                final_delay_series = delay_metrics_dict["final"]

                for dezena_val in ALL_NUMBERS:
                    all_metrics_data.append({
                        'chunk_seq_id': idx + 1, 
                        'chunk_start_contest': start_contest,
                        'chunk_end_contest': end_contest,
                        'dezena': int(dezena_val),
                        'frequencia_absoluta': int(frequency_series.get(dezena_val, 0)),
                        'atraso_medio_no_bloco': float(mean_delay_series.get(dezena_val, np.nan)) if pd.notna(mean_delay_series.get(dezena_val, np.nan)) else None,
                        'atraso_maximo_no_bloco': int(max_delay_series.get(dezena_val, 0)),
                        'atraso_final_no_bloco': int(final_delay_series.get(dezena_val, 0))
                    })

            if not all_metrics_data:
                logger.warning(f"Nenhuma métrica de chunk calculada para {chunk_type}_{size}.")
                continue

            # Criar um DataFrame único e depois separar por métrica para salvar
            metrics_df_long = pd.DataFrame(all_metrics_data)

            metric_columns_to_save = {
                "frequency": "frequencia_absoluta",
                "atraso_medio_bloco": "atraso_medio_no_bloco",
                "atraso_maximo_bloco": "atraso_maximo_no_bloco",
                "atraso_final_bloco": "atraso_final_no_bloco"
            }
            base_cols = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest', 'dezena']

            for metric_key, value_col_name in metric_columns_to_save.items():
                df_to_save = metrics_df_long[base_cols + [value_col_name]].copy()
                table_name = f"evol_metric_{metric_key}_{chunk_type}_{size}"
                try:
                    if df_to_save[value_col_name].isnull().all() and value_col_name == "atraso_medio_no_bloco":
                        logger.info(f"Coluna '{value_col_name}' contém apenas NaNs para '{table_name}'. Salvando como está.")
                    elif df_to_save[value_col_name].isnull().any() and value_col_name == "atraso_medio_no_bloco":
                         logger.debug(f"Coluna '{value_col_name}' para '{table_name}' contém alguns NaNs.")

                    db_manager.save_dataframe_to_db(df_to_save, table_name, if_exists='replace')
                    logger.info(f"Métrica '{metric_key}' para '{chunk_type}_{size}' salva em '{table_name}'. {len(df_to_save)} registros.")
                except Exception as e:
                    logger.error(f"Erro ao salvar '{metric_key}' para '{chunk_type}_{size}' em '{table_name}': {e}", exc_info=True)
            
    logger.info("Cálculo e persistência de métricas de chunk (otimizado) concluídos.")