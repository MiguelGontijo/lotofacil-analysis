# Lotofacil_Analysis/src/analysis/chunk_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any, Set, Optional
import logging
import numpy as np
import math

from src.analysis.number_properties_analysis import analyze_draw_properties

logger = logging.getLogger(__name__)

def get_chunk_definitions(
    total_contests: int,
    chunk_type_from_config: str,
    chunk_sizes_from_config: List[int],
    config: Any
) -> List[Tuple[int, int, str, int]]:
    definitions: List[Tuple[int, int, str, int]] = []
    if not hasattr(config, 'CHUNK_TYPES_CONFIG'):
        logger.error("CHUNK_TYPES_CONFIG não encontrado no config.")
        return definitions

    if not chunk_sizes_from_config:
        logger.warning(f"Nenhum tamanho para tipo de bloco: {chunk_type_from_config}")
        return definitions

    for sz_item in chunk_sizes_from_config:
        if sz_item <= 0:
            logger.warning(f"Tamanho de chunk inválido: {sz_item} para {chunk_type_from_config}. Pulando.")
            continue
        current_pos = 0
        chunk_idx_for_type_size = 0

        while current_pos < total_contests:
            start_contest = current_pos + 1
            end_contest = min(current_pos + sz_item, total_contests)
            chunk_seq_id = chunk_idx_for_type_size + 1
            chunk_label = f"{chunk_type_from_config}_{sz_item}"
            definitions.append((start_contest, end_contest, chunk_label, chunk_seq_id))
            current_pos = end_contest
            chunk_idx_for_type_size +=1
            if start_contest > end_contest :
                logger.warning(f"Chunk inválido start > end ({start_contest} > {end_contest}). Interrompendo.")
                break
    logger.debug(f"Defs de chunk para '{chunk_type_from_config}', tamanhos={chunk_sizes_from_config}: {len(definitions)} blocos.")
    return definitions

def calculate_frequency_in_chunk(df_chunk: pd.DataFrame, config: Any) -> pd.Series:
    if not hasattr(config, 'ALL_NUMBERS') or not hasattr(config, 'BALL_NUMBER_COLUMNS'):
        logger.error("ALL_NUMBERS ou BALL_NUMBER_COLUMNS não em config para calculate_frequency_in_chunk.")
        return pd.Series(dtype='int', index=pd.Index([], name="dezena"), name="frequencia_absoluta")

    if df_chunk.empty:
        return pd.Series(dtype='int').reindex(config.ALL_NUMBERS, fill_value=0).rename("frequencia_absoluta").rename_axis("dezena")

    dezena_cols = config.BALL_NUMBER_COLUMNS
    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
    if not actual_dezena_cols:
        logger.warning(f"Nenhuma coluna de bola ({dezena_cols}) no chunk para frequência.")
        return pd.Series(dtype='int').reindex(config.ALL_NUMBERS, fill_value=0).rename("frequencia_absoluta").rename_axis("dezena")

    all_drawn_numbers_in_chunk_list = []
    for col in actual_dezena_cols:
        all_drawn_numbers_in_chunk_list.extend(pd.to_numeric(df_chunk[col], errors='coerce').dropna().astype(int).tolist())

    if not all_drawn_numbers_in_chunk_list:
        return pd.Series(dtype='int').reindex(config.ALL_NUMBERS, fill_value=0).rename("frequencia_absoluta").rename_axis("dezena")

    frequency_series = pd.Series(all_drawn_numbers_in_chunk_list).value_counts()
    frequency_series = frequency_series.reindex(config.ALL_NUMBERS, fill_value=0)
    frequency_series.name = "frequencia_absoluta"
    frequency_series.index.name = "dezena"
    return frequency_series.astype(int)


def get_draw_matrix_for_chunk(df_chunk: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int, config: Any) -> pd.DataFrame:
    default_cols = getattr(config, 'ALL_NUMBERS', list(range(1,26)))
    contest_id_col_name = getattr(config, 'CONTEST_ID_COLUMN_NAME', 'contest_id')

    if not hasattr(config, 'BALL_NUMBER_COLUMNS'):
        logger.error("BALL_NUMBER_COLUMNS não em config para get_draw_matrix_for_chunk.")
        return pd.DataFrame(columns=default_cols)

    all_contests_in_chunk_range = pd.Index(range(chunk_start_contest, chunk_end_contest + 1), name=contest_id_col_name)

    if df_chunk.empty:
        logger.debug(f"Chunk C{chunk_start_contest}-C{chunk_end_contest} vazio. Retornando matriz de zeros.")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)

    dezena_cols = config.BALL_NUMBER_COLUMNS
    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
    if not actual_dezena_cols:
        logger.warning(f"Nenhuma coluna de bola ({dezena_cols}) em df_chunk para C{chunk_start_contest}-C{chunk_end_contest}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)

    if contest_id_col_name not in df_chunk.columns:
        logger.error(f"Coluna '{contest_id_col_name}' ausente em df_chunk para C{chunk_start_contest}-C{chunk_end_contest}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)

    df_chunk_copy = df_chunk[[contest_id_col_name] + actual_dezena_cols].copy()
    try:
        df_chunk_copy[contest_id_col_name] = pd.to_numeric(df_chunk_copy[contest_id_col_name])
    except Exception as e:
        logger.error(f"Não converter '{contest_id_col_name}' para numérico: {e}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)

    melted_df = df_chunk_copy.melt(id_vars=[contest_id_col_name], value_vars=actual_dezena_cols, value_name='Dezena_val_temp')
    melted_df.dropna(subset=['Dezena_val_temp'], inplace=True)
    if melted_df.empty:
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)

    melted_df['Dezena'] = pd.to_numeric(melted_df['Dezena_val_temp'], errors='coerce')
    melted_df.dropna(subset=['Dezena'], inplace=True)
    if melted_df.empty:
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)
    melted_df['Dezena'] = melted_df['Dezena'].astype(int)
    melted_df['presente'] = 1

    try:
        draw_matrix = melted_df.pivot_table(index=contest_id_col_name, columns='Dezena', values='presente', fill_value=0)
        draw_matrix = draw_matrix.reindex(index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, fill_value=0)
    except Exception as e:
        logger.error(f"Erro ao pivotar draw_matrix no chunk C{chunk_start_contest}-C{chunk_end_contest}: {e}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)
    return draw_matrix.astype(int)

def calculate_delays_for_matrix(draw_matrix: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int, config: Any) -> Dict[str, pd.Series]:
    chunk_duration_calc = chunk_end_contest - chunk_start_contest + 1
    results: Dict[str, pd.Series] = {
        "final": pd.Series(chunk_duration_calc, index=config.ALL_NUMBERS, dtype='Int64', name="atraso_final_no_bloco"),
        "mean": pd.Series(np.nan, index=config.ALL_NUMBERS, dtype='float', name="atraso_medio_no_bloco"),
        "max": pd.Series(chunk_duration_calc, index=config.ALL_NUMBERS, dtype='Int64', name="atraso_maximo_no_bloco"),
        "std_dev": pd.Series(np.nan, index=config.ALL_NUMBERS, dtype='float', name="delay_std_dev")
    }
    if draw_matrix.empty:
        logger.debug(f"Matriz de sorteios vazia para atrasos C{chunk_start_contest}-C{chunk_end_contest}.")
        return results

    draw_matrix.index = pd.to_numeric(draw_matrix.index)
    draw_matrix = draw_matrix.sort_index()

    if not draw_matrix.index.unique().tolist(): # Verificação se há algum índice após conversão
        return results

    for dezena_val_loop in config.ALL_NUMBERS:
        if dezena_val_loop not in draw_matrix.columns:
            logger.debug(f"Dezena {dezena_val_loop} não na draw_matrix para C{chunk_start_contest}-C{chunk_end_contest}.")
            continue

        col_dezena = draw_matrix[dezena_val_loop]
        occurrence_contests = col_dezena[col_dezena == 1].index.sort_values().tolist()

        if not occurrence_contests:
            continue # Atraso final e máximo permanecem como chunk_duration_calc

        results["final"].loc[dezena_val_loop] = chunk_end_contest - occurrence_contests[-1]

        gaps: List[int] = [occurrence_contests[0] - chunk_start_contest]
        for i in range(len(occurrence_contests) - 1):
            gaps.append(occurrence_contests[i+1] - occurrence_contests[i] - 1)
        gaps.append(chunk_end_contest - occurrence_contests[-1])

        if gaps:
            results["mean"].loc[dezena_val_loop] = np.mean(gaps) if gaps else np.nan
            results["max"].loc[dezena_val_loop] = max(gaps) if gaps else chunk_duration_calc
            current_gaps_std = pd.Series(gaps, dtype=float).std(ddof=0) if len(gaps) > 1 else np.nan
            results["std_dev"].loc[dezena_val_loop] = current_gaps_std
    return results

def calculate_block_group_summary_metrics(df_chunk: pd.DataFrame, config: Any) -> Dict[str, Optional[float]]:
    summary_metrics: Dict[str, Optional[float]] = {
        "avg_pares_no_bloco": None, "avg_impares_no_bloco": None,
        "avg_primos_no_bloco": None, "avg_soma_dezenas_no_bloco": None
    }
    if df_chunk.empty or not hasattr(config, 'BALL_NUMBER_COLUMNS') or not hasattr(config, 'NUMBERS_PER_DRAW'):
        return summary_metrics

    ball_cols = config.BALL_NUMBER_COLUMNS
    actual_ball_cols = [col for col in ball_cols if col in df_chunk.columns]
    if not actual_ball_cols: return summary_metrics

    contest_properties_list: List[Dict[str, Any]] = []
    for _, row in df_chunk.iterrows():
        try:
            draw_numbers_raw = [row[col] for col in actual_ball_cols if pd.notna(row[col])]
            draw = [int(float(d_num)) for d_num in draw_numbers_raw if str(d_num).replace('.0','',1).isdigit()]

            if len(draw) == config.NUMBERS_PER_DRAW:
                properties = analyze_draw_properties(draw, config)
                contest_properties_list.append(properties)
            elif len(draw_numbers_raw) > 0:
                 logger.debug(f"Sorteio incompleto no concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'N/A')} (bloco). Dezenas: {draw_numbers_raw}.")
        except ValueError as e_val:
            logger.warning(f"Erro converter dezenas para concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'Desconhecido')} (grupo): {e_val}.")
            continue
        except Exception as e_gen:
             logger.error(f"Erro inesperado em group_summary para concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'Desconhecido')}: {e_gen}")
             continue

    if not contest_properties_list: return summary_metrics
    df_contest_properties = pd.DataFrame(contest_properties_list)

    property_to_final_col_map = {
        'pares': 'avg_pares_no_bloco', 'impares': 'avg_impares_no_bloco',
        'primos': 'avg_primos_no_bloco', 'soma_dezenas': 'avg_soma_dezenas_no_bloco'
    }
    for prop_key, final_col_name in property_to_final_col_map.items():
        if prop_key in df_contest_properties.columns and not df_contest_properties[prop_key].empty:
            mean_val = df_contest_properties[prop_key].mean()
            summary_metrics[final_col_name] = round(mean_val, 2) if pd.notna(mean_val) else None
    return summary_metrics

def calculate_chunk_metrics_and_persist(all_data_df: pd.DataFrame, db_manager: Any, config: Any):
    logger.info("Iniciando cálculo e persistência de métricas de chunk.")
    contest_col = config.CONTEST_ID_COLUMN_NAME

    if contest_col not in all_data_df.columns:
        logger.error(f"Coluna '{contest_col}' não em all_data_df para chunk_metrics. Abortando.")
        return

    df_to_process = all_data_df.copy()
    try:
        df_to_process[contest_col] = pd.to_numeric(df_to_process[contest_col], errors='coerce')
        df_to_process.dropna(subset=[contest_col], inplace=True)
        if df_to_process.empty:
            logger.error("DataFrame vazio após limpar coluna de concurso. Abortando.")
            return
        df_to_process[contest_col] = df_to_process[contest_col].astype(int)
        if df_to_process[contest_col].empty:
             logger.error("Coluna de concurso vazia após conversão. Abortando.")
             return
        total_contests = df_to_process[contest_col].max()
    except Exception as e_conv:
        logger.error(f"Erro ao processar '{contest_col}': {e_conv}. Abortando.")
        return

    if pd.isna(total_contests) or total_contests <= 0:
        logger.error(f"Total de concursos inválido: {total_contests}. Abortando.")
        return

    for chunk_type_key, list_of_sizes in config.CHUNK_TYPES_CONFIG.items():
        for size_val_loop in list_of_sizes:
            logger.info(f"Processando chunks: tipo='{chunk_type_key}', tamanho={size_val_loop}.")
            chunk_definitions = get_chunk_definitions(int(total_contests), chunk_type_key, [size_val_loop], config)

            if not chunk_definitions:
                logger.warning(f"Nenhuma definição de chunk para {chunk_type_key}_{size_val_loop}."); continue

            all_metrics_for_db: List[Dict[str, Any]] = []
            all_group_metrics_for_db: List[Dict[str, Any]] = []

            for start_contest, end_contest, chunk_label_original, chunk_seq_id_val in chunk_definitions:
                mask = (df_to_process[contest_col] >= start_contest) & (df_to_process[contest_col] <= end_contest)
                df_current_chunk = df_to_process[mask]
                if df_current_chunk.empty:
                    logger.debug(f"Chunk C{start_contest}-C{end_contest} (SeqID {chunk_seq_id_val}) vazio. Pulando.")
                    continue
                chunk_actual_duration = end_contest - start_contest + 1
                frequency_series = calculate_frequency_in_chunk(df_current_chunk, config)
                draw_matrix_chunk = get_draw_matrix_for_chunk(df_current_chunk, start_contest, end_contest, config)
                delay_metrics_dict = calculate_delays_for_matrix(draw_matrix_chunk, start_contest, end_contest, config)
                for dezena_val_loop in config.ALL_NUMBERS:
                    frequencia_abs_dezena = frequency_series.get(dezena_val_loop, 0)
                    freq_rel_no_chunk = frequencia_abs_dezena / chunk_actual_duration if chunk_actual_duration > 0 else 0.0
                    occurrence_std_dev_val = math.sqrt(freq_rel_no_chunk * (1 - freq_rel_no_chunk)) if 0 < freq_rel_no_chunk < 1 else 0.0
                    all_metrics_for_db.append({
                        'chunk_seq_id': chunk_seq_id_val, 'chunk_start_contest': start_contest,
                        'chunk_end_contest': end_contest, 'dezena': int(dezena_val_loop),
                        'frequencia_absoluta': int(frequencia_abs_dezena),
                        'atraso_medio_no_bloco': float(delay_metrics_dict["mean"].get(dezena_val_loop, np.nan)) if pd.notna(delay_metrics_dict["mean"].get(dezena_val_loop, np.nan)) else None,
                        'atraso_maximo_no_bloco': int(delay_metrics_dict["max"].get(dezena_val_loop, chunk_actual_duration)) if pd.notna(delay_metrics_dict["max"].get(dezena_val_loop, chunk_actual_duration)) else None,
                        'atraso_final_no_bloco': int(delay_metrics_dict["final"].get(dezena_val_loop, chunk_actual_duration)) if pd.notna(delay_metrics_dict["final"].get(dezena_val_loop, chunk_actual_duration)) else None,
                        'occurrence_std_dev': round(occurrence_std_dev_val, 6) if pd.notna(occurrence_std_dev_val) else None,
                        'delay_std_dev': round(float(delay_metrics_dict["std_dev"].get(dezena_val_loop, np.nan)), 6) if pd.notna(delay_metrics_dict["std_dev"].get(dezena_val_loop, np.nan)) else None
                    })
                group_metrics = calculate_block_group_summary_metrics(df_current_chunk, config)
                all_group_metrics_for_db.append({'chunk_seq_id': chunk_seq_id_val, 'chunk_start_contest': start_contest, 'chunk_end_contest': end_contest, **group_metrics})

            if all_metrics_for_db:
                metrics_df_long_combined = pd.DataFrame(all_metrics_for_db)
                base_cols_dezena = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest', 'dezena']
                metrics_to_save_map = {
                    config.EVOL_METRIC_FREQUENCY_BLOCK_PREFIX: "frequencia_absoluta",
                    config.EVOL_METRIC_ATRASO_MEDIO_BLOCK_PREFIX: "atraso_medio_no_bloco",
                    config.EVOL_METRIC_ATRASO_MAXIMO_BLOCK_PREFIX: "atraso_maximo_no_bloco",
                    config.EVOL_METRIC_ATRASO_FINAL_BLOCK_PREFIX: "atraso_final_no_bloco",
                    config.EVOL_METRIC_OCCURRENCE_STD_DEV_BLOCK_PREFIX: "occurrence_std_dev",
                    config.EVOL_METRIC_DELAY_STD_DEV_BLOCK_PREFIX: "delay_std_dev"
                }
                for table_prefix_from_config, value_col_name_in_df in metrics_to_save_map.items():
                    if value_col_name_in_df in metrics_df_long_combined.columns:
                        df_to_save_metric = metrics_df_long_combined[base_cols_dezena + [value_col_name_in_df]].copy()
                        if pd.api.types.is_float_dtype(df_to_save_metric[value_col_name_in_df]):
                             df_to_save_metric.dropna(subset=[value_col_name_in_df], inplace=True)
                        if not df_to_save_metric.empty:
                            table_name = f"{table_prefix_from_config}_{chunk_type_key}_{size_val_loop}"
                            # CORREÇÃO APLICADA: removido index=False
                            db_manager.save_dataframe(df_to_save_metric, table_name, if_exists='replace')
                            logger.info(f"Métricas ({value_col_name_in_df}) salvas em '{table_name}'. {len(df_to_save_metric)} regs.")
                        else:
                            logger.debug(f"Nenhum dado para métrica '{value_col_name_in_df}' no chunk {chunk_type_key}_{size_val_loop}.")
            if all_group_metrics_for_db:
                group_metrics_df = pd.DataFrame(all_group_metrics_for_db)
                cols_to_check_for_nan_group = [col for col in group_metrics_df.columns if col.startswith('avg_')]
                if cols_to_check_for_nan_group :
                     group_metrics_df.dropna(subset=cols_to_check_for_nan_group, how='all', inplace=True)
                if not group_metrics_df.empty:
                    group_table_name = f"{config.EVOL_BLOCK_GROUP_METRICS_PREFIX}_{chunk_type_key}_{size_val_loop}"
                    # CORREÇÃO APLICADA: removido index=False
                    db_manager.save_dataframe(group_metrics_df, group_table_name, if_exists='replace')
                    logger.info(f"Métricas de grupo de chunk salvas em '{group_table_name}'. {len(group_metrics_df)} regs.")
                else:
                    logger.info(f"Nenhuma métrica de grupo de chunk para {chunk_type_key}_{size_val_loop}.")
    logger.info("Cálculo e persistência de métricas de chunk concluído.")