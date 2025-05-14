# src/analysis/chunk_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any, Set, Optional # <<< 'Optional' ADICIONADO AQUI
import logging
import numpy as np

from src.config import ALL_NUMBERS, CHUNK_TYPES_CONFIG
from src.database_manager import DatabaseManager
from src.analysis.number_properties_analysis import analyze_draw_properties 

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

def get_draw_matrix_for_chunk(df_chunk: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int) -> pd.DataFrame:
    all_contests_in_chunk_range = pd.Index(range(chunk_start_contest, chunk_end_contest + 1), name='Concurso')
    if df_chunk.empty:
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=ALL_NUMBERS, dtype=int)
    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
    if not actual_dezena_cols:
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=ALL_NUMBERS, dtype=int)
    
    # Verifica se 'Concurso' existe, senão não pode ser usado como id_vars
    if 'Concurso' not in df_chunk.columns:
        logger.error("Coluna 'Concurso' ausente em df_chunk para get_draw_matrix_for_chunk.")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=ALL_NUMBERS, dtype=int)

    melted_df = df_chunk.melt(id_vars=['Concurso'], value_vars=actual_dezena_cols, value_name='Dezena')
    melted_df.dropna(subset=['Dezena'], inplace=True)
    if melted_df.empty: 
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=ALL_NUMBERS, dtype=int)
    melted_df['Dezena'] = melted_df['Dezena'].astype(int)
    melted_df['presente'] = 1
    try:
        draw_matrix = melted_df.pivot_table(index='Concurso', columns='Dezena', values='presente', fill_value=0)
        draw_matrix = draw_matrix.reindex(index=all_contests_in_chunk_range, columns=ALL_NUMBERS, fill_value=0)
    except Exception as e:
        logger.error(f"Erro ao pivotar dados para draw_matrix no chunk C{chunk_start_contest}-C{chunk_end_contest}: {e}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=ALL_NUMBERS, dtype=int)
    return draw_matrix.astype(int)

def calculate_delays_for_matrix(draw_matrix: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int) -> Dict[str, pd.Series]:
    chunk_duration = chunk_end_contest - chunk_start_contest + 1
    results: Dict[str, pd.Series] = { # Type hint explícito para results
        "final": pd.Series(chunk_duration, index=ALL_NUMBERS, dtype='int', name="atraso_final_no_bloco"),
        "mean": pd.Series(float(chunk_duration), index=ALL_NUMBERS, dtype='float', name="atraso_medio_no_bloco"),
        "max": pd.Series(chunk_duration, index=ALL_NUMBERS, dtype='int', name="atraso_maximo_no_bloco")
    }
    if draw_matrix.empty: 
        logger.debug(f"Matriz de sorteios vazia para C{chunk_start_contest}-C{chunk_end_contest}. Atrasos serão duração do chunk.")
        return results
        
    for dezena_val in ALL_NUMBERS: 
        if dezena_val not in draw_matrix.columns: 
            logger.warning(f"Dezena {dezena_val} não encontrada na draw_matrix para C{chunk_start_contest}-C{chunk_end_contest}.")
            continue
        col_dezena = draw_matrix[dezena_val]
        occurrence_contests = col_dezena[col_dezena == 1].index.to_list()
        if not occurrence_contests: 
            # Atrasos permanecem como chunk_duration (já definidos na inicialização de results)
            continue
        
        results["final"].loc[dezena_val] = chunk_end_contest - occurrence_contests[-1]
        gaps: List[int] = [] # Type hint explícito
        gaps.append(occurrence_contests[0] - (chunk_start_contest - 1) - 1)
        for i in range(len(occurrence_contests) - 1):
            gaps.append(occurrence_contests[i+1] - occurrence_contests[i] - 1)
        gaps.append(chunk_end_contest - occurrence_contests[-1])
        
        if gaps: 
            results["mean"].loc[dezena_val] = np.mean(gaps) if gaps else float(chunk_duration) # Evita mean de lista vazia
            results["max"].loc[dezena_val] = max(gaps) if gaps else chunk_duration
        # else: não deve acontecer se occurrence_contests não for vazio.
            
    return results

def calculate_block_group_summary_metrics(df_chunk: pd.DataFrame) -> Dict[str, Optional[float]]:
    summary_metrics: Dict[str, Optional[float]] = { "avg_pares_no_bloco": None, "avg_impares_no_bloco": None, "avg_primos_no_bloco": None }
    if df_chunk.empty: 
        logger.debug("Chunk vazio, métricas de grupo não calculadas (retornando None para todas).")
        return summary_metrics
        
    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
    if not actual_dezena_cols:
        logger.warning("Nenhuma coluna de bola no chunk para calcular métricas de grupo.")
        return summary_metrics

    contest_properties_list: List[Dict[str, Any]] = []
    for _, row in df_chunk.iterrows():
        try:
            draw = [int(row[col]) for col in actual_dezena_cols if pd.notna(row[col])]
            if len(draw) == 15: 
                properties = analyze_draw_properties(draw) 
                contest_properties_list.append(properties)
        except ValueError: 
            logger.warning(f"Erro converter dezenas no concurso {row.get('Concurso', 'Desconhecido')} para group metrics."); 
            continue # Pula este sorteio, mas continua com outros no chunk
            
    if not contest_properties_list:
        logger.debug("Nenhuma propriedade de sorteio válida calculada para o chunk (retornando None para todas).")
        return summary_metrics

    df_contest_properties = pd.DataFrame(contest_properties_list)
    
    if 'pares' in df_contest_properties.columns:
        summary_metrics["avg_pares_no_bloco"] = df_contest_properties['pares'].mean()
    if 'impares' in df_contest_properties.columns:
        summary_metrics["avg_impares_no_bloco"] = df_contest_properties['impares'].mean()
    if 'primos' in df_contest_properties.columns:
        summary_metrics["avg_primos_no_bloco"] = df_contest_properties['primos'].mean()
        
    return summary_metrics

def calculate_chunk_metrics_and_persist(all_data_df: pd.DataFrame, db_manager: DatabaseManager):
    logger.info("Iniciando cálculo de métricas de chunk (Frequência, Atrasos e Métricas de Grupo).")
    if 'Concurso' not in all_data_df.columns: 
        logger.error("Coluna 'Concurso' não encontrada."); return
    total_contests = all_data_df['Concurso'].max()
    if pd.isna(total_contests) or total_contests <= 0: 
        logger.error(f"Total de concursos inválido: {total_contests}."); return

    for chunk_type, config in CHUNK_TYPES_CONFIG.items():
        chunk_sizes = config.get('sizes', [])
        for size in chunk_sizes:
            logger.info(f"Processando chunks: tipo='{chunk_type}', tamanho={size} para todas as métricas.")
            chunk_definitions = get_chunk_definitions(total_contests, chunk_type, [size])
            if not chunk_definitions: 
                logger.warning(f"Nenhuma definição de chunk para {chunk_type}_{size}."); continue

            all_metrics_data: List[Dict[str, Any]] = []
            block_group_metrics_data: List[Dict[str, Any]] = []
            
            for idx, (start_contest, end_contest, _) in enumerate(chunk_definitions):
                mask = (all_data_df['Concurso'] >= start_contest) & (all_data_df['Concurso'] <= end_contest)
                df_current_chunk = all_data_df[mask]

                frequency_series = calculate_frequency_in_chunk(df_current_chunk)
                draw_matrix_chunk = get_draw_matrix_for_chunk(df_current_chunk, start_contest, end_contest)
                delay_metrics_dict = calculate_delays_for_matrix(draw_matrix_chunk, start_contest, end_contest)
                
                for dezena_val in ALL_NUMBERS:
                    all_metrics_data.append({
                        'chunk_seq_id': idx + 1, 'chunk_start_contest': start_contest, 'chunk_end_contest': end_contest,
                        'dezena': int(dezena_val),
                        'frequencia_absoluta': int(frequency_series.get(dezena_val, 0)),
                        'atraso_medio_no_bloco': float(delay_metrics_dict["mean"].get(dezena_val, np.nan)) if pd.notna(delay_metrics_dict["mean"].get(dezena_val, np.nan)) else None,
                        'atraso_maximo_no_bloco': int(delay_metrics_dict["max"].get(dezena_val, chunk_duration if df_current_chunk.empty or draw_matrix_chunk.empty else 0)), # Default ajustado
                        'atraso_final_no_bloco': int(delay_metrics_dict["final"].get(dezena_val, chunk_duration if df_current_chunk.empty or draw_matrix_chunk.empty else 0))  # Default ajustado
                    })
                
                group_metrics = calculate_block_group_summary_metrics(df_current_chunk)
                block_group_metrics_data.append({
                    'chunk_seq_id': idx + 1, 'chunk_start_contest': start_contest, 'chunk_end_contest': end_contest,
                    'avg_pares_no_bloco': group_metrics.get("avg_pares_no_bloco"),
                    'avg_impares_no_bloco': group_metrics.get("avg_impares_no_bloco"),
                    'avg_primos_no_bloco': group_metrics.get("avg_primos_no_bloco")
                })

            if all_metrics_data:
                metrics_df_long = pd.DataFrame(all_metrics_data)
                metric_columns_to_save = {"frequency": "frequencia_absoluta", "atraso_medio_bloco": "atraso_medio_no_bloco", "atraso_maximo_bloco": "atraso_maximo_no_bloco", "atraso_final_bloco": "atraso_final_no_bloco"}
                base_cols = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest', 'dezena']
                for metric_key, value_col_name in metric_columns_to_save.items():
                    if value_col_name not in metrics_df_long.columns: # Segurança
                        logger.warning(f"Coluna de valor '{value_col_name}' não encontrada em metrics_df_long para {metric_key}. Pulando salvamento.")
                        continue
                    df_to_save = metrics_df_long[base_cols + [value_col_name]].copy()
                    table_name = f"evol_metric_{metric_key}_{chunk_type}_{size}"
                    try: db_manager.save_dataframe_to_db(df_to_save, table_name); logger.info(f"'{table_name}' salva ({len(df_to_save)} reg).")
                    except Exception as e: logger.error(f"Erro salvar '{table_name}': {e}", exc_info=True)
            
            if block_group_metrics_data:
                group_metrics_df = pd.DataFrame(block_group_metrics_data)
                group_table_name = f"evol_block_group_metrics_{chunk_type}_{size}"
                try: db_manager.save_dataframe_to_db(group_metrics_df, group_table_name); logger.info(f"'{group_table_name}' salva ({len(group_metrics_df)} reg).")
                except Exception as e: logger.error(f"Erro salvar '{group_table_name}': {e}", exc_info=True)
    logger.info("Cálculo de métricas de chunk (incluindo de grupo) concluído.")