# src/analysis/chunk_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any, Set, Optional
import logging
import numpy as np
import math

# Importa de number_properties_analysis DENTRO da função para evitar import circular
# from src.analysis.number_properties_analysis import analyze_draw_properties

logger = logging.getLogger(__name__)

def get_chunk_definitions(total_contests: int, chunk_type_from_config: str, chunk_sizes_from_config: List[int], config: Any) -> List[Tuple[int, int, str]]:
    definitions: List[Tuple[int, int, str]] = []
    if not chunk_sizes_from_config:
        logger.warning(f"Nenhum tamanho configurado para o tipo de bloco: {chunk_type_from_config}")
        return definitions
    for sz_item in chunk_sizes_from_config:
        if sz_item <= 0:
            logger.warning(f"Tamanho de chunk inválido: {sz_item} para o tipo {chunk_type_from_config}. Pulando.")
            continue
        current_pos = 0

        while current_pos < total_contests:
            start_contest = current_pos + 1 
            end_contest = min(current_pos + sz_item, total_contests)
            # Se o chunk resultante for menor que o tamanho do chunk (acontece no final)
            # e você quiser apenas chunks completos, adicione uma verificação aqui:
            # if (end_contest - start_contest + 1) < sz_item:
            #     break 
            definitions.append((start_contest, end_contest, f"{chunk_type_from_config}_{sz_item}")) 
            current_pos = end_contest 
            if start_contest > end_contest : 
                logger.warning(f"Condição de chunk inválida start_contest > end_contest ({start_contest} > {end_contest}). Interrompendo para este tamanho.")
                break
    logger.debug(f"Definições de chunk para tipo='{chunk_type_from_config}', tamanhos={chunk_sizes_from_config}: {len(definitions)} blocos gerados.")
    return definitions

def calculate_frequency_in_chunk(df_chunk: pd.DataFrame, config: Any) -> pd.Series:
    if df_chunk.empty:
        return pd.Series(dtype='int').reindex(config.ALL_NUMBERS, fill_value=0).rename("frequencia_absoluta").rename_axis("dezena")

    dezena_cols = config.BALL_NUMBER_COLUMNS
    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
    if not actual_dezena_cols:
        logger.warning(f"Nenhuma coluna de bola ({dezena_cols}) encontrada no chunk para calcular frequência.")
        return pd.Series(dtype='int').reindex(config.ALL_NUMBERS, fill_value=0).rename("frequencia_absoluta").rename_axis("dezena")

    all_drawn_numbers_in_chunk_list = []
    for col in actual_dezena_cols:
        all_drawn_numbers_in_chunk_list.extend(pd.to_numeric(df_chunk[col], errors='coerce').dropna().astype(int).tolist())
    
    if not all_drawn_numbers_in_chunk_list: # Se após coletar e limpar, a lista está vazia
        return pd.Series(dtype='int').reindex(config.ALL_NUMBERS, fill_value=0).rename("frequencia_absoluta").rename_axis("dezena")

    frequency_series = pd.Series(all_drawn_numbers_in_chunk_list).value_counts()
    frequency_series = frequency_series.reindex(config.ALL_NUMBERS, fill_value=0)
    frequency_series.name = "frequencia_absoluta" 
    frequency_series.index.name = "dezena" 
    return frequency_series.astype(int)

def get_draw_matrix_for_chunk(df_chunk: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int, config: Any) -> pd.DataFrame:
    all_contests_in_chunk_range = pd.Index(range(chunk_start_contest, chunk_end_contest + 1), name=config.CONTEST_ID_COLUMN_NAME)
    
    if df_chunk.empty:
        logger.debug(f"DataFrame do chunk C{chunk_start_contest}-C{chunk_end_contest} vazio. Retornando matriz de zeros.")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)

    dezena_cols = config.BALL_NUMBER_COLUMNS
    contest_col = config.CONTEST_ID_COLUMN_NAME

    actual_dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
    if not actual_dezena_cols:
        logger.warning(f"Nenhuma coluna de bola ({dezena_cols}) encontrada em df_chunk para get_draw_matrix_for_chunk. C{chunk_start_contest}-C{chunk_end_contest}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)
    
    if contest_col not in df_chunk.columns:
        logger.error(f"Coluna '{contest_col}' ausente em df_chunk para get_draw_matrix_for_chunk. C{chunk_start_contest}-C{chunk_end_contest}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)

    df_chunk_copy = df_chunk[[contest_col] + actual_dezena_cols].copy()
    try:
        df_chunk_copy[contest_col] = pd.to_numeric(df_chunk_copy[contest_col])
    except Exception as e:
        logger.error(f"Não foi possível converter '{contest_col}' para numérico em get_draw_matrix_for_chunk: {e}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)

    melted_df = df_chunk_copy.melt(id_vars=[contest_col], value_vars=actual_dezena_cols, value_name='Dezena')
    melted_df.dropna(subset=['Dezena'], inplace=True) # Remove linhas onde 'Dezena' é NaN (após melt, se alguma coluna de bola era NaN)
    if melted_df.empty: 
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)
    
    melted_df['Dezena'] = pd.to_numeric(melted_df['Dezena'], errors='coerce') # Converte para numérico, NaNs se não puder
    melted_df.dropna(subset=['Dezena'], inplace=True) # Remove NaNs após conversão
    if melted_df.empty: 
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)
    melted_df['Dezena'] = melted_df['Dezena'].astype(int) # Agora seguro para converter para int

    melted_df['presente'] = 1
    
    try:
        draw_matrix = melted_df.pivot_table(index=contest_col, columns='Dezena', values='presente', fill_value=0)
        # Reindexa para incluir todos os concursos no range e todas as dezenas possíveis
        draw_matrix = draw_matrix.reindex(index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, fill_value=0)
    except Exception as e:
        logger.error(f"Erro ao pivotar dados para draw_matrix no chunk C{chunk_start_contest}-C{chunk_end_contest}: {e}")
        return pd.DataFrame(0, index=all_contests_in_chunk_range, columns=config.ALL_NUMBERS, dtype=int)
    return draw_matrix.astype(int)

def calculate_delays_for_matrix(draw_matrix: pd.DataFrame, chunk_start_contest: int, chunk_end_contest: int, config: Any) -> Dict[str, pd.Series]:
    chunk_duration_calc = chunk_end_contest - chunk_start_contest + 1 # Usar esta variável localmente
    results: Dict[str, pd.Series] = {
        "final": pd.Series(chunk_duration_calc, index=config.ALL_NUMBERS, dtype='Int64', name="atraso_final_no_bloco"),
        "mean": pd.Series(np.nan, index=config.ALL_NUMBERS, dtype='float', name="atraso_medio_no_bloco"), 
        "max": pd.Series(chunk_duration_calc, index=config.ALL_NUMBERS, dtype='Int64', name="atraso_maximo_no_bloco"),
        "std_dev": pd.Series(np.nan, index=config.ALL_NUMBERS, dtype='float', name="atraso_std_dev_no_bloco")
    }
    if draw_matrix.empty: 
        logger.debug(f"Matriz de sorteios vazia para cálculo de atrasos em C{chunk_start_contest}-C{chunk_end_contest}.")
        return results
        
    draw_matrix.index = pd.to_numeric(draw_matrix.index)
    draw_matrix = draw_matrix.sort_index()
    
    # IDs de concurso reais no chunk, para determinar o último concurso EFETIVO no chunk
    actual_contests_in_chunk = draw_matrix.index.tolist()
    if not actual_contests_in_chunk:
        return results # Retorna defaults se não houver concursos na matriz


    for dezena_val in config.ALL_NUMBERS: 
        if dezena_val not in draw_matrix.columns: 
            logger.warning(f"Dezena {dezena_val} não encontrada na draw_matrix para C{chunk_start_contest}-C{chunk_end_contest}.")
            # results["std_dev"].loc[dezena_val] já é np.nan
            continue 
        
        col_dezena = draw_matrix[dezena_val]
        occurrence_contests = col_dezena[col_dezena == 1].index.sort_values().tolist()
        
        if not occurrence_contests: 
            # Os valores default já foram setados na inicialização de `results`
            continue
        
        results["final"].loc[dezena_val] = chunk_end_contest - occurrence_contests[-1]
        
        gaps: List[int] = []
        # Gap inicial: da borda inicial do chunk até a primeira ocorrência DENTRO do chunk
        gaps.append(occurrence_contests[0] - chunk_start_contest)
        
        for i in range(len(occurrence_contests) - 1):
            gaps.append(occurrence_contests[i+1] - occurrence_contests[i] - 1) 
            
        # Gap final: da última ocorrência DENTRO do chunk até a borda final do chunk
        gaps.append(chunk_end_contest - occurrence_contests[-1])
        
        if gaps: 
            results["mean"].loc[dezena_val] = np.mean(gaps)
            results["max"].loc[dezena_val] = max(gaps)
            current_delay_std = pd.Series(gaps).std(ddof=1) 
            results["std_dev"].loc[dezena_val] = current_delay_std # pd.Series.std() já retorna NaN se apropriado
        else: 
            # Este caso (gaps vazio) não deve ocorrer se occurrence_contests não for vazio
            # devido à forma como os gaps são calculados (incluindo gaps das bordas).
            # Mas, por segurança, se ocorrer:
            results["mean"].loc[dezena_val] = np.nan 
            results["max"].loc[dezena_val] = chunk_duration_calc # Atraso máximo é a duração se não houve gaps internos (ou só uma ocorrência)
            results["std_dev"].loc[dezena_val] = np.nan
            
    return results

def calculate_block_group_summary_metrics(df_chunk: pd.DataFrame, config: Any) -> Dict[str, Optional[float]]:
    from src.analysis.number_properties_analysis import analyze_draw_properties 

    summary_metrics: Dict[str, Optional[float]] = { "avg_pares_no_bloco": None, "avg_impares_no_bloco": None, "avg_primos_no_bloco": None }
    if df_chunk.empty: return summary_metrics
        
    ball_cols = config.BALL_NUMBER_COLUMNS
    actual_ball_cols = [col for col in ball_cols if col in df_chunk.columns]
    if not actual_ball_cols: return summary_metrics

    contest_properties_list: List[Dict[str, Any]] = []
    for _, row in df_chunk.iterrows():
        try:
            draw_numbers_str_list = [row[col] for col in actual_ball_cols if pd.notna(row[col])] # Coleta como string ou num
            draw = [int(d_num) for d_num in draw_numbers_str_list] # Converte para int
            
            if len(draw) == config.NUMBERS_PER_DRAW: 
                properties = analyze_draw_properties(draw, config) 
                contest_properties_list.append(properties)
        except ValueError as e_val: 
            logger.warning(f"Erro ao converter dezenas para o concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'Desconhecido')} para métricas de grupo: {e_val}. Dezenas: {draw_numbers_str_list if 'draw_numbers_str_list' in locals() else 'N/A'}")
            continue
        except Exception as e_gen: # Captura outras exceções inesperadas
             logger.error(f"Erro inesperado em calculate_block_group_summary_metrics para concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'Desconhecido')}: {e_gen}")
             continue
            
    if not contest_properties_list: return summary_metrics
    df_contest_properties = pd.DataFrame(contest_properties_list)
    
    for group_col_name_prop in ['pares', 'impares', 'primos']: 
        db_col_name_avg = f"avg_{group_col_name_prop}_no_bloco"
        if group_col_name_prop in df_contest_properties.columns and not df_contest_properties[group_col_name_prop].empty:
            summary_metrics[db_col_name_avg] = df_contest_properties[group_col_name_prop].mean()
            
    return summary_metrics

def calculate_chunk_metrics_and_persist(all_data_df: pd.DataFrame, db_manager: Any, config: Any):
    logger.info("Iniciando cálculo de métricas de chunk.")
    contest_col = config.CONTEST_ID_COLUMN_NAME
    if contest_col not in all_data_df.columns: 
        logger.error(f"Coluna '{contest_col}' não encontrada em all_data_df para chunk_metrics."); return
        
    df_to_process = all_data_df.copy()
    try:
        df_to_process[contest_col] = pd.to_numeric(df_to_process[contest_col], errors='coerce')
        df_to_process.dropna(subset=[contest_col], inplace=True)
        df_to_process[contest_col] = df_to_process[contest_col].astype(int)
        if df_to_process.empty: 
            logger.error("DataFrame vazio após limpar coluna de concurso em calculate_chunk_metrics_and_persist."); return
        total_contests = df_to_process[contest_col].max() 
    except Exception as e_conv: 
        logger.error(f"Erro ao processar coluna '{contest_col}' em calculate_chunk_metrics_and_persist: {e_conv}"); return

    if pd.isna(total_contests) or total_contests <= 0: 
        logger.error(f"Total de concursos inválido: {total_contests} em calculate_chunk_metrics_and_persist."); return

    for chunk_type, list_of_sizes in config.CHUNK_TYPES_CONFIG.items():
        for size_val in list_of_sizes:
            logger.info(f"Processando chunks: tipo='{chunk_type}', tamanho={size_val}.")
            chunk_definitions = get_chunk_definitions(int(total_contests), chunk_type, [size_val], config) 
            if not chunk_definitions: 
                logger.warning(f"Nenhuma definição de chunk para {chunk_type}_{size_val}."); continue

            all_metrics_for_db: List[Dict[str, Any]] = []
            all_group_metrics_for_db: List[Dict[str, Any]] = []
            
            for idx, (start_contest, end_contest, _) in enumerate(chunk_definitions):
                chunk_seq_id = idx + 1
                mask = (df_to_process[contest_col] >= start_contest) & (df_to_process[contest_col] <= end_contest)
                df_current_chunk = df_to_process[mask]

                if df_current_chunk.empty: 
                    logger.debug(f"Chunk C{start_contest}-C{end_contest} (SeqID {chunk_seq_id}) está vazio. Pulando.")
                    continue
                
                # Define a duração do chunk aqui para uso nos defaults
                chunk_actual_duration = end_contest - start_contest + 1

                frequency_series = calculate_frequency_in_chunk(df_current_chunk, config)
                draw_matrix_chunk = get_draw_matrix_for_chunk(df_current_chunk, start_contest, end_contest, config)
                delay_metrics_dict = calculate_delays_for_matrix(draw_matrix_chunk, start_contest, end_contest, config) 
                
                for dezena_val in config.ALL_NUMBERS:
                    frequencia_abs_dezena = frequency_series.get(dezena_val, 0)
                    # Usar a duração real do chunk para freq_rel é mais consistente
                    # num_draws_no_chunk_calc = len(df_current_chunk) # se os dados puderem ter buracos
                    num_draws_no_chunk_calc = chunk_actual_duration 
                    
                    freq_rel_dezena = 0.0
                    if num_draws_no_chunk_calc > 0:
                        freq_rel_dezena = frequencia_abs_dezena / num_draws_no_chunk_calc
                    
                    current_occurrence_std_dev = 0.0
                    if 0 < freq_rel_dezena < 1: 
                        current_occurrence_std_dev = math.sqrt(freq_rel_dezena * (1 - freq_rel_dezena))

                    all_metrics_for_db.append({
                        'chunk_seq_id': chunk_seq_id, 'chunk_start_contest': start_contest, 'chunk_end_contest': end_contest,
                        'dezena': int(dezena_val),
                        'frequencia_absoluta': int(frequencia_abs_dezena), 
                        'atraso_medio_no_bloco': float(delay_metrics_dict["mean"].get(dezena_val, np.nan)) if pd.notna(delay_metrics_dict["mean"].get(dezena_val, np.nan)) else None,
                        # <<< CORREÇÃO: Usando chunk_actual_duration para o default >>>
                        'atraso_maximo_no_bloco': int(delay_metrics_dict["max"].get(dezena_val, chunk_actual_duration)),
                        'atraso_final_no_bloco': int(delay_metrics_dict["final"].get(dezena_val, chunk_actual_duration)),
                        'occurrence_std_dev': current_occurrence_std_dev, 
                        'delay_std_dev': float(delay_metrics_dict["std_dev"].get(dezena_val, np.nan)) if pd.notna(delay_metrics_dict["std_dev"].get(dezena_val, np.nan)) else None 
                    })
                
                group_metrics = calculate_block_group_summary_metrics(df_current_chunk, config)
                all_group_metrics_for_db.append({
                    'chunk_seq_id': chunk_seq_id, 'chunk_start_contest': start_contest, 'chunk_end_contest': end_contest,
                    'avg_pares_no_bloco': group_metrics.get("avg_pares_no_bloco"),
                    'avg_impares_no_bloco': group_metrics.get("avg_impares_no_bloco"),
                    'avg_primos_no_bloco': group_metrics.get("avg_primos_no_bloco")
                })

            if all_metrics_for_db:
                metrics_df_long = pd.DataFrame(all_metrics_for_db)
                metric_value_cols = {
                    "frequency": "frequencia_absoluta", 
                    "atraso_medio_bloco": "atraso_medio_no_bloco", 
                    "atraso_maximo_bloco": "atraso_maximo_no_bloco", 
                    "atraso_final_bloco": "atraso_final_no_bloco",
                    "occurrence_std_dev_bloco": "occurrence_std_dev", 
                    "delay_std_dev_bloco": "delay_std_dev"            
                }
                base_cols = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest', 'dezena']
                for metric_name, value_col_original_name in metric_value_cols.items(): 
                    if value_col_original_name in metrics_df_long.columns: 
                        df_to_save = metrics_df_long[base_cols + [value_col_original_name]].copy()
                        # <<< LINHA DE RENAME FOI REMOVIDA PARA CORRIGIR O ERRO DO BLOCK_AGGREGATOR >>>
                        table_name = f"evol_metric_{metric_name}_{chunk_type}_{size_val}"
                        db_manager.save_dataframe(df_to_save, table_name, if_exists='replace')
                        logger.info(f"Salvo em '{table_name}' com coluna de valor '{value_col_original_name}'.")
            
            if all_group_metrics_for_db:
                group_metrics_df = pd.DataFrame(all_group_metrics_for_db)
                group_table_name = f"evol_block_group_metrics_{chunk_type}_{size_val}"
                db_manager.save_dataframe(group_metrics_df, group_table_name, if_exists='replace')
                logger.info(f"Salvo em '{group_table_name}'.")
    logger.info("Cálculo de métricas de chunk concluído.")