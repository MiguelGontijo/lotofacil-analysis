# Lotofacil_Analysis/src/analysis/rank_trend_analysis.py
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Optional, Any
from scipy.stats import linregress

logger = logging.getLogger(__name__)

def calculate_and_persist_rank_per_chunk(db_manager: Any, config: Any) -> None:
    logger.info("Iniciando cálculo e persistência de ranking de frequência por chunk.")

    freq_table_prefix = config.EVOL_METRIC_FREQUENCY_BLOCK_PREFIX
    rank_table_prefix = config.EVOL_RANK_FREQUENCY_BLOCK_PREFIX
    chunk_seq_id_col = 'chunk_seq_id'
    dezena_col = config.DEZENA_COLUMN_NAME
    freq_col = 'frequencia_absoluta'
    rank_col = 'rank_no_bloco'

    if not hasattr(config, 'CHUNK_TYPES_CONFIG'):
        logger.error("CHUNK_TYPES_CONFIG não encontrado no objeto de configuração.")
        return

    for chunk_type, sizes in config.CHUNK_TYPES_CONFIG.items():
        for size in sizes:
            logger.info(f"Processando ranking para chunks: tipo='{chunk_type}', tamanho={size}")
            freq_table_name = f"{freq_table_prefix}_{chunk_type}_{size}"
            rank_table_name = f"{rank_table_prefix}_{chunk_type}_{size}"

            if not db_manager.table_exists(freq_table_name):
                logger.warning(f"Tabela de frequência '{freq_table_name}' não encontrada. Pulando rank para {chunk_type}_{size}.")
                continue

            df_freq_chunk = db_manager.load_dataframe(freq_table_name)

            if df_freq_chunk is None or df_freq_chunk.empty:
                logger.info(f"DataFrame de frequência para '{freq_table_name}' vazio. Pulando rank.")
                empty_rank_df_cols = [chunk_seq_id_col, dezena_col, rank_col]
                if 'chunk_start_contest' in df_freq_chunk.columns:
                     empty_rank_df_cols.insert(1, 'chunk_start_contest')
                if 'chunk_end_contest' in df_freq_chunk.columns:
                     empty_rank_df_cols.insert(2, 'chunk_end_contest')
                empty_rank_df = pd.DataFrame(columns=empty_rank_df_cols)
                try:
                    # CORREÇÃO: Removido index=False (se existia aqui, mas o erro principal é abaixo)
                    db_manager.save_dataframe(empty_rank_df, rank_table_name, if_exists='replace')
                except Exception as e_save_empty:
                    logger.error(f"Erro ao salvar tabela de rank vazia '{rank_table_name}': {e_save_empty}")
                continue

            if freq_col not in df_freq_chunk.columns:
                logger.error(f"Coluna de frequência '{freq_col}' não encontrada em '{freq_table_name}'. Pulando rank.")
                continue
            
            df_freq_chunk[rank_col] = df_freq_chunk.groupby(chunk_seq_id_col)[freq_col].rank(method='dense', ascending=False).astype(int)
            
            cols_to_save = [chunk_seq_id_col]
            if 'chunk_start_contest' in df_freq_chunk.columns:
                cols_to_save.append('chunk_start_contest')
            if 'chunk_end_contest' in df_freq_chunk.columns:
                cols_to_save.append('chunk_end_contest')
            cols_to_save.extend([dezena_col, rank_col])
            
            df_to_save = df_freq_chunk[[col for col in cols_to_save if col in df_freq_chunk.columns]].copy()

            try:
                # CORREÇÃO APLICADA: removido index=False
                db_manager.save_dataframe(df_to_save, rank_table_name, if_exists='replace')
                logger.info(f"Dados de ranking salvos na tabela '{rank_table_name}'.")
            except Exception as e_save:
                logger.error(f"Erro ao salvar dados de ranking na tabela '{rank_table_name}': {e_save}", exc_info=True)
                
    logger.info("Cálculo e persistência de ranking de frequência por chunk concluídos.")


def calculate_historical_rank_trends(
    db_manager: Any,
    config: Any,
    aggregated_block_table_name: str,
    rank_analysis_type_filter: str,
    rank_value_column_name: str, 
    trend_window_blocks: int,
    slope_improving_threshold: float,
    slope_worsening_threshold: float
) -> Optional[pd.DataFrame]:
    logger.info(f"Iniciando cálculo de tendências de rank da tabela: {aggregated_block_table_name} filtrando por tipo_analise='{rank_analysis_type_filter}'.")

    if not db_manager.table_exists(aggregated_block_table_name):
        logger.error(f"Tabela agregada de blocos '{aggregated_block_table_name}' não encontrada.")
        return None

    df_agg_blocks = db_manager.load_dataframe(aggregated_block_table_name)
    if df_agg_blocks is None or df_agg_blocks.empty:
        logger.warning(f"DataFrame da tabela agregada de blocos '{aggregated_block_table_name}' está vazio.")
        return None

    df_ranks = df_agg_blocks[df_agg_blocks['tipo_analise'] == rank_analysis_type_filter].copy()
    
    if df_ranks.empty:
        logger.warning(f"Nenhum dado encontrado para tipo_analise='{rank_analysis_type_filter}' na tabela '{aggregated_block_table_name}'.")
        return None
        
    all_trend_data: List[Dict[str, Any]] = []
    
    required_trend_cols = ['chunk_seq_id', 'chunk_end_contest']
    if not all(col in df_ranks.columns for col in required_trend_cols):
        logger.error(f"Colunas {required_trend_cols} não encontradas no DataFrame de ranks. Não é possível calcular tendências.")
        return None
        
    for dezena_num in config.ALL_NUMBERS:
        dezena_col_for_rank_value = f"dezena_{dezena_num}"
        
        if dezena_col_for_rank_value not in df_ranks.columns:
            logger.debug(f"Coluna de rank '{dezena_col_for_rank_value}' não encontrada para dezena {dezena_num}. Pulando.")
            last_chunk_end_contest = df_ranks['chunk_end_contest'].max() if not df_ranks.empty else None
            if last_chunk_end_contest is not None:
                all_trend_data.append({
                    config.CONTEST_ID_COLUMN_NAME: int(last_chunk_end_contest),
                    config.DEZENA_COLUMN_NAME: int(dezena_num),
                    config.RANK_SLOPE_COLUMN_NAME: np.nan,
                    config.TREND_STATUS_COLUMN_NAME: "sem_dados_rank"
                })
            continue

        df_dezena_rank_history = df_ranks[['chunk_seq_id', 'chunk_end_contest', dezena_col_for_rank_value]].copy()
        df_dezena_rank_history.rename(columns={dezena_col_for_rank_value: 'rank_value'}, inplace=True)
        df_dezena_rank_history.dropna(subset=['rank_value'], inplace=True)
        df_dezena_rank_history = df_dezena_rank_history.sort_values(by='chunk_seq_id')
        
        slope = np.nan
        trend_status = "indefinido"
        target_contest_id = None
        
        if not df_dezena_rank_history.empty:
            target_contest_id = int(df_dezena_rank_history['chunk_end_contest'].iloc[-1]) if pd.notna(df_dezena_rank_history['chunk_end_contest'].iloc[-1]) else None

            if len(df_dezena_rank_history) >= 2:
                window_to_use = min(trend_window_blocks, len(df_dezena_rank_history))
                if window_to_use < 2:
                     trend_status = "insuficiente_janela"
                else:
                    df_window = df_dezena_rank_history.tail(window_to_use)
                    x_values = df_window['chunk_seq_id'].values
                    y_values = pd.to_numeric(df_window['rank_value'], errors='coerce').values
                    
                    valid_mask = ~np.isnan(y_values)
                    x_values_clean = x_values[valid_mask]
                    y_values_clean = y_values[valid_mask]

                    if len(x_values_clean) >= 2:
                        regression_result = linregress(x_values_clean, y_values_clean)
                        slope = regression_result.slope
                        
                        if pd.notna(slope):
                            if slope < slope_improving_threshold: 
                                trend_status = "melhorando"
                            elif slope > slope_worsening_threshold: 
                                trend_status = "piorando"
                            else:
                                trend_status = "estavel"
                        else:
                            trend_status = "indefinido_slope_nan"
                    else:
                        trend_status = "insuficiente_apos_limpeza_nan"
            else: 
                trend_status = "insuficiente_pontos_historico"
        else: 
             trend_status = "sem_historico_de_rank_para_dezena"
             if not df_ranks.empty and 'chunk_end_contest' in df_ranks.columns:
                 target_contest_id = int(df_ranks['chunk_end_contest'].max()) if pd.notna(df_ranks['chunk_end_contest'].max()) else None

        if target_contest_id is not None:
            all_trend_data.append({
                config.CONTEST_ID_COLUMN_NAME: target_contest_id,
                config.DEZENA_COLUMN_NAME: int(dezena_num),
                config.RANK_SLOPE_COLUMN_NAME: round(slope, 4) if pd.notna(slope) else None,
                config.TREND_STATUS_COLUMN_NAME: trend_status
            })
        
    if not all_trend_data:
        logger.warning("Nenhum dado de tendência de rank foi gerado após processar todas as dezenas.")
        return pd.DataFrame(columns=[config.CONTEST_ID_COLUMN_NAME, config.DEZENA_COLUMN_NAME, config.RANK_SLOPE_COLUMN_NAME, config.TREND_STATUS_COLUMN_NAME])
        
    df_final_trends = pd.DataFrame(all_trend_data)
    df_final_trends.dropna(subset=[config.CONTEST_ID_COLUMN_NAME], inplace=True)
    if not df_final_trends.empty:
        df_final_trends[config.CONTEST_ID_COLUMN_NAME] = df_final_trends[config.CONTEST_ID_COLUMN_NAME].astype(int)

    logger.info(f"Cálculo de tendências de rank (slope/status) concluído. {len(df_final_trends)} registros gerados.")
    return df_final_trends