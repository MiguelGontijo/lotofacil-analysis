# src/analysis/block_aggregator.py
import pandas as pd
import logging
from typing import Dict, List, Any, Optional
import numpy as np

# Removidas importações diretas de config, serão acessadas via objeto 'config'
# from src.database_manager import DatabaseManager # Será passado como argumento

logger = logging.getLogger(__name__)

def aggregate_block_data_to_wide_format(db_manager: Any, config: Any): # Recebe config
    logger.info("Iniciando agregação de dados de bloco para formato largo (incluindo métricas de grupo).")
    per_dezena_metric_configs = [
        {"source_table_prefix": "evol_metric_frequency", "value_column": "frequencia_absoluta", "analysis_type_name": "frequencia_bloco", "dtype": "Int64"},
        {"source_table_prefix": "evol_rank_frequency_bloco", "value_column": "rank_no_bloco", "analysis_type_name": "rank_freq_bloco", "dtype": "Int64"},
        {"source_table_prefix": "evol_metric_atraso_medio_bloco", "value_column": "atraso_medio_no_bloco", "analysis_type_name": "atraso_medio_bloco", "dtype": "float"},
        {"source_table_prefix": "evol_metric_atraso_maximo_bloco", "value_column": "atraso_maximo_no_bloco", "analysis_type_name": "atraso_maximo_bloco", "dtype": "Int64"},
        {"source_table_prefix": "evol_metric_atraso_final_bloco", "value_column": "atraso_final_no_bloco", "analysis_type_name": "atraso_final_bloco", "dtype": "Int64"},
    ]
    
    # Acessa CHUNK_TYPES_CONFIG através do objeto config passado como parâmetro
    for chunk_type, list_of_sizes in config.CHUNK_TYPES_CONFIG.items(): # CORRIGIDO AQUI
        # list_of_sizes é a lista de inteiros, ex: [10, 25, 50]
        for size_val in list_of_sizes: # Renomeado para evitar conflito com nome de módulo 'size'
            consolidated_table_name = f"bloco_analises_consolidadas_{chunk_type}_{size_val}"
            logger.info(f"Processando para tabela consolidada de BLOCKS: '{consolidated_table_name}'")
            all_wide_dfs_for_this_chunk_config: List[pd.DataFrame] = []
            
            for metric_config in per_dezena_metric_configs:
                source_table_prefix = metric_config["source_table_prefix"]
                value_column_in_long_table = metric_config["value_column"]
                analysis_type_name_for_wide_table = metric_config["analysis_type_name"]
                expected_dtype_str = metric_config["dtype"]
                long_format_table_name = f"{source_table_prefix}_{chunk_type}_{size_val}"
                
                df_long = db_manager.load_dataframe(long_format_table_name) # CORRIGIDO para load_dataframe

                if df_long is None or df_long.empty:
                    logger.warning(f"DataFrame de '{long_format_table_name}' vazio para {analysis_type_name_for_wide_table}. Pulando.")
                    continue
                
                index_cols = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest']
                columns_col = 'dezena'
                required_for_pivot = index_cols + [columns_col, value_column_in_long_table]

                if not all(col in df_long.columns for col in required_for_pivot):
                    logger.error(f"Tabela '{long_format_table_name}' não tem colunas para pivotar {required_for_pivot}. Colunas presentes: {df_long.columns.tolist()}. Pulando.")
                    continue
                try:
                    if expected_dtype_str == "Int64":
                        df_long[value_column_in_long_table] = pd.to_numeric(df_long[value_column_in_long_table], errors='coerce')
                    elif expected_dtype_str == "float":
                        df_long[value_column_in_long_table] = pd.to_numeric(df_long[value_column_in_long_table], errors='coerce')
                    
                    df_wide_metric = df_long.pivot_table(
                        index=index_cols, 
                        columns=columns_col, 
                        values=value_column_in_long_table, 
                        fill_value=pd.NA if expected_dtype_str == "Int64" else np.nan
                    )
                    df_wide_metric.columns = [f'dezena_{int(col)}' for col in df_wide_metric.columns]
                    df_wide_metric.reset_index(inplace=True)
                    df_wide_metric['tipo_analise'] = analysis_type_name_for_wide_table
                    
                    # Usa config.ALL_NUMBERS
                    final_cols_order = index_cols + ['tipo_analise'] + [f'dezena_{i}' for i in config.ALL_NUMBERS]
                    for i in config.ALL_NUMBERS:
                        col_name_to_check = f'dezena_{i}'
                        if col_name_to_check not in df_wide_metric.columns:
                            df_wide_metric[col_name_to_check] = pd.NA if expected_dtype_str == "Int64" else np.nan
                        
                        if expected_dtype_str == "Int64":
                            df_wide_metric[col_name_to_check] = df_wide_metric[col_name_to_check].astype('Int64')
                        elif expected_dtype_str == "float":
                            df_wide_metric[col_name_to_check] = df_wide_metric[col_name_to_check].astype('float')
                    
                    # Garantir que apenas colunas existentes em final_cols_order sejam selecionadas
                    actual_final_cols = [col for col in final_cols_order if col in df_wide_metric.columns]
                    all_wide_dfs_for_this_chunk_config.append(df_wide_metric[actual_final_cols])
                except Exception as e:
                    logger.error(f"Erro ao pivotar '{long_format_table_name}' para '{analysis_type_name_for_wide_table}': {e}", exc_info=True)
            
            block_group_metrics_table_name = f"evol_block_group_metrics_{chunk_type}_{size_val}"
            df_block_group_metrics = db_manager.load_dataframe(block_group_metrics_table_name) # CORRIGIDO

            if df_block_group_metrics is None or df_block_group_metrics.empty: 
                logger.warning(f"Tabela de métricas de grupo '{block_group_metrics_table_name}' não encontrada ou vazia.")
                df_block_group_metrics = None 
            
            if all_wide_dfs_for_this_chunk_config:
                df_consolidated_wide = pd.concat(all_wide_dfs_for_this_chunk_config, ignore_index=True)
                if df_block_group_metrics is not None:
                    merge_keys = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest']
                    if all(key in df_consolidated_wide.columns for key in merge_keys) and all(key in df_block_group_metrics.columns for key in merge_keys):
                        df_consolidated_wide = pd.merge(df_consolidated_wide, df_block_group_metrics, on=merge_keys, how='left')
                    else: logger.warning(f"Chaves de merge não encontradas para '{consolidated_table_name}'. Métricas de grupo de bloco não adicionadas.")
                try:
                    db_manager.save_dataframe(df_consolidated_wide, consolidated_table_name, if_exists='replace')
                    logger.info(f"Tabela consolidada de BLOCKS '{consolidated_table_name}' salva com {len(df_consolidated_wide)} linhas.")
                except Exception as e: logger.error(f"Erro ao salvar tabela consolidada de BLOCKS '{consolidated_table_name}': {e}", exc_info=True)
            elif df_block_group_metrics is not None: 
                 logger.info(f"Salvando apenas métricas de grupo para BLOCKS '{consolidated_table_name}'.")
                 db_manager.save_dataframe(df_block_group_metrics, consolidated_table_name, if_exists='replace')
            else: logger.warning(f"Nenhum DataFrame (por dezena ou grupo) gerado para BLOCKS '{consolidated_table_name}'.")
    logger.info("Agregação de dados de bloco para formato largo concluída.")


def aggregate_cycle_data_to_wide_format(db_manager: Any, config: Any): # Recebe config
    logger.info("Iniciando agregação de dados de CICLO para formato largo (incluindo rank e métricas de grupo).")
    cycle_per_dezena_metric_configs = [
        {"source_table": "ciclo_metric_frequency", "value_column": "frequencia_no_ciclo", "analysis_type_name": "frequencia_no_ciclo", "dtype": "Int64"},
        {"source_table": "ciclo_metric_atraso_medio", "value_column": "atraso_medio_no_ciclo", "analysis_type_name": "atraso_medio_no_ciclo", "dtype": "float"},
        {"source_table": "ciclo_metric_atraso_maximo", "value_column": "atraso_maximo_no_ciclo", "analysis_type_name": "atraso_maximo_no_ciclo", "dtype": "Int64"},
        {"source_table": "ciclo_metric_atraso_final", "value_column": "atraso_final_no_ciclo", "analysis_type_name": "atraso_final_no_ciclo", "dtype": "Int64"},
        {"source_table": "ciclo_rank_frequency", "value_column": "rank_freq_no_ciclo", "analysis_type_name": "rank_freq_no_ciclo", "dtype": "Int64"},
    ]
    consolidated_cycle_table_name = "ciclo_analises_consolidadas"
    all_wide_dfs_for_cycles: List[pd.DataFrame] = []

    df_ciclos_detalhe = db_manager.load_dataframe("ciclos_detalhe") # CORRIGIDO
        
    if df_ciclos_detalhe is None or df_ciclos_detalhe.empty: 
        logger.warning("Tabela 'ciclos_detalhe' não encontrada ou vazia. Não é possível criar tabela consolidada de ciclos.")
        return 
    
    df_closed_ciclos_base_info = df_ciclos_detalhe[df_ciclos_detalhe['concurso_fim'].notna()].copy()
    if df_closed_ciclos_base_info.empty:
        logger.warning("Nenhum ciclo fechado em 'ciclos_detalhe'. Tabela consolidada de ciclos não será gerada/atualizada.")
        return
        
    df_closed_ciclos_base_info.rename(columns={'concurso_inicio': 'concurso_inicio_ciclo', 'concurso_fim': 'concurso_fim_ciclo', 'duracao_concursos': 'duracao_ciclo'}, inplace=True)
    df_closed_ciclos_base_info = df_closed_ciclos_base_info[['ciclo_num', 'concurso_inicio_ciclo', 'concurso_fim_ciclo', 'duracao_ciclo']]

    for metric_config in cycle_per_dezena_metric_configs:
        long_format_table_name = metric_config["source_table"]
        value_column_in_long_table = metric_config["value_column"]
        analysis_type_name_for_wide_table = metric_config["analysis_type_name"]
        expected_dtype_str = metric_config["dtype"]
        
        df_long_cycle_metric = db_manager.load_dataframe(long_format_table_name) # CORRIGIDO

        if df_long_cycle_metric is None or df_long_cycle_metric.empty:
            logger.warning(f"DataFrame de '{long_format_table_name}' vazio. Pulando."); continue
        
        index_cols_cycle = ['ciclo_num']; columns_col_cycle = 'dezena'
        required_for_pivot_cycle = index_cols_cycle + [columns_col_cycle, value_column_in_long_table]
        if not all(col in df_long_cycle_metric.columns for col in required_for_pivot_cycle) :
            logger.error(f"Tabela '{long_format_table_name}' não tem colunas para pivotar {required_for_pivot_cycle}. Colunas: {df_long_cycle_metric.columns.tolist()}. Pulando.")
            continue
        try:
            if expected_dtype_str == "Int64":
                df_long_cycle_metric[value_column_in_long_table] = pd.to_numeric(df_long_cycle_metric[value_column_in_long_table], errors='coerce')
            elif expected_dtype_str == "float":
                df_long_cycle_metric[value_column_in_long_table] = pd.to_numeric(df_long_cycle_metric[value_column_in_long_table], errors='coerce')
            
            df_wide_metric = df_long_cycle_metric.pivot_table(
                index=index_cols_cycle, columns=columns_col_cycle, 
                values=value_column_in_long_table, 
                fill_value=pd.NA if expected_dtype_str == "Int64" else np.nan
            )
            df_wide_metric.columns = [f'dezena_{int(col)}' for col in df_wide_metric.columns]
            df_wide_metric.reset_index(inplace=True)
            df_wide_metric['tipo_analise_ciclo'] = analysis_type_name_for_wide_table
            df_wide_metric_merged = pd.merge(df_closed_ciclos_base_info, df_wide_metric, on='ciclo_num', how='inner')
            
            # Usa config.ALL_NUMBERS
            final_cols_order = ['ciclo_num', 'concurso_inicio_ciclo', 'concurso_fim_ciclo', 'duracao_ciclo', 'tipo_analise_ciclo'] + [f'dezena_{i}' for i in config.ALL_NUMBERS]
            for i in config.ALL_NUMBERS:
                col_name_to_check = f'dezena_{i}'
                if col_name_to_check not in df_wide_metric_merged.columns:
                    df_wide_metric_merged[col_name_to_check] = pd.NA if expected_dtype_str == "Int64" else np.nan
                
                if expected_dtype_str == "Int64":
                    df_wide_metric_merged[col_name_to_check] = df_wide_metric_merged[col_name_to_check].astype('Int64')
                elif expected_dtype_str == "float":
                     df_wide_metric_merged[col_name_to_check] = df_wide_metric_merged[col_name_to_check].astype('float')
            
            actual_final_cols = [col for col in final_cols_order if col in df_wide_metric_merged.columns]
            all_wide_dfs_for_cycles.append(df_wide_metric_merged[actual_final_cols])
        except Exception as e:
            logger.error(f"Erro ao pivotar '{long_format_table_name}' para '{analysis_type_name_for_wide_table}': {e}", exc_info=True)
    
    df_consolidated_cycles_wide = pd.DataFrame()
    if all_wide_dfs_for_cycles:
        df_consolidated_cycles_wide = pd.concat(all_wide_dfs_for_cycles, ignore_index=True)

    cycle_group_metrics_table_name = "ciclo_group_metrics"
    df_cycle_group_metrics = db_manager.load_dataframe(cycle_group_metrics_table_name) # CORRIGIDO
        
    if df_cycle_group_metrics is None or df_cycle_group_metrics.empty:
        logger.warning(f"Tabela de métricas de grupo de ciclo '{cycle_group_metrics_table_name}' vazia ou não encontrada.")
        df_cycle_group_metrics = None

    if df_cycle_group_metrics is not None:
        if not df_consolidated_cycles_wide.empty:
            if 'ciclo_num' in df_consolidated_cycles_wide.columns and 'ciclo_num' in df_cycle_group_metrics.columns:
                 df_consolidated_cycles_wide = pd.merge(df_consolidated_cycles_wide, df_cycle_group_metrics, on='ciclo_num', how='left')
                 logger.info(f"Métricas de grupo de ciclo adicionadas. Shape após merge: {df_consolidated_cycles_wide.shape}")
            else:
                logger.warning("Coluna 'ciclo_num' não encontrada para merge de métricas de grupo de ciclo.")
        else: 
            logger.info("Construindo tabela consolidada de ciclos apenas com métricas de grupo.")
            if 'ciclo_num' in df_closed_ciclos_base_info.columns and 'ciclo_num' in df_cycle_group_metrics.columns:
                df_consolidated_cycles_wide = pd.merge(df_closed_ciclos_base_info, df_cycle_group_metrics, on='ciclo_num', how='inner')
                if not all_wide_dfs_for_cycles: 
                    logger.warning("Tabela consolidada de ciclos não será criada apenas com métricas de grupo se não houver métricas por dezena.")
                    df_consolidated_cycles_wide = pd.DataFrame() 

    if not df_consolidated_cycles_wide.empty:
        try:
            db_manager.save_dataframe(df_consolidated_cycles_wide, consolidated_cycle_table_name, if_exists='replace')
            logger.info(f"Tabela consolidada de CICLOS '{consolidated_cycle_table_name}' salva com {len(df_consolidated_cycles_wide)} linhas.")
        except Exception as e:
            logger.error(f"Erro ao salvar tabela consolidada de CICLOS '{consolidated_cycle_table_name}': {e}", exc_info=True)
    else:
        logger.warning(f"Nenhum DataFrame consolidado gerado para CICLOS. Tabela '{consolidated_cycle_table_name}' não foi criada/atualizada.")
    logger.info("Agregação de dados de CICLO para formato largo (com métricas de grupo) concluída.")