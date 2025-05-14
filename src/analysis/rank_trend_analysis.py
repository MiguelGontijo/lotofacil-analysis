# src/analysis/rank_trend_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any
import logging

# Removidas importações diretas de config, serão acessadas via objeto 'config'
# from src.database_manager import DatabaseManager # Será passado como argumento

logger = logging.getLogger(__name__)

def calculate_and_persist_rank_per_chunk(db_manager: Any, config: Any): # Recebe config
    logger.info("Iniciando cálculo e persistência de ranking de frequência por chunk.")

    # Acessa CHUNK_TYPES_CONFIG através do objeto config passado como parâmetro
    for chunk_type, list_of_sizes in config.CHUNK_TYPES_CONFIG.items(): # CORRIGIDO AQUI
        # list_of_sizes é a lista de inteiros, ex: [10, 25, 50]
        for size_val in list_of_sizes: # Renomeado para evitar conflito
            freq_table_name = f"evol_metric_frequency_{chunk_type}_{size_val}"
            rank_table_name = f"evol_rank_frequency_bloco_{chunk_type}_{size_val}"
            
            logger.info(f"Processando ranking para chunks: tipo='{chunk_type}', tamanho={size_val}")
            logger.debug(f"Lendo da tabela de frequência: '{freq_table_name}', Salvando ranking em: '{rank_table_name}'")

            df_freq_chunk = db_manager.load_dataframe(freq_table_name) # CORRIGIDO

            if df_freq_chunk is None or df_freq_chunk.empty:
                logger.warning(f"DataFrame de frequência carregado de '{freq_table_name}' está vazio. Pulando cálculo de ranking.")
                continue
            
            required_cols = ['chunk_seq_id', 'dezena', 'frequencia_absoluta', 'chunk_start_contest', 'chunk_end_contest']
            if not all(col in df_freq_chunk.columns for col in required_cols):
                logger.error(f"Tabela '{freq_table_name}' não contém colunas {required_cols}. Colunas: {df_freq_chunk.columns.tolist()}. Pulando.")
                continue

            try:
                df_freq_chunk['rank_no_bloco'] = df_freq_chunk.groupby('chunk_seq_id')['frequencia_absoluta'] \
                                                              .rank(method='dense', ascending=False).astype(int)
            except Exception as e:
                logger.error(f"Erro ao calcular o rank para a tabela '{freq_table_name}': {e}", exc_info=True)
                continue
                
            df_to_save = df_freq_chunk.rename(columns={'frequencia_absoluta': 'frequencia_no_bloco'})
            
            cols_order = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest', 
                          'dezena', 'frequencia_no_bloco', 'rank_no_bloco']
            
            actual_cols_order = [col for col in cols_order if col in df_to_save.columns]
            df_to_save = df_to_save[actual_cols_order]

            try:
                db_manager.save_dataframe(df_to_save, rank_table_name, if_exists='replace')
                logger.info(f"Ranking de frequência por chunk salvo na tabela '{rank_table_name}'. {len(df_to_save)} registros.")
            except Exception as e:
                logger.error(f"Erro ao salvar dados de ranking na tabela '{rank_table_name}': {e}", exc_info=True)

    logger.info("Cálculo e persistência de ranking de frequência por chunk concluídos.")


def calculate_and_persist_general_rank(db_manager: Any, config: Any): # Recebe config
    logger.info("Iniciando cálculo e persistência de ranking geral de dezenas por frequência.")
    freq_abs_table_name = "frequencia_absoluta" 

    df_freq_abs = db_manager.load_dataframe(freq_abs_table_name) # CORRIGIDO

    if df_freq_abs is None or df_freq_abs.empty:
        logger.warning(f"DataFrame da tabela '{freq_abs_table_name}' está vazio. Não é possível calcular rank geral.")
        return
    
    # Os nomes das colunas em 'frequencia_absoluta' são 'Dezena' e 'Frequencia Absoluta'
    dezena_col_name = "Dezena"
    freq_col_name = "Frequencia Absoluta"

    if not dezena_col_name in df_freq_abs.columns or not freq_col_name in df_freq_abs.columns:
        logger.error(f"Tabela '{freq_abs_table_name}' não contém '{dezena_col_name}' ou '{freq_col_name}'. Colunas: {df_freq_abs.columns.tolist()}")
        return

    df_freq_abs['rank_geral'] = df_freq_abs[freq_col_name].rank(method='dense', ascending=False).astype(int)
    
    df_rank_geral = df_freq_abs[[dezena_col_name, freq_col_name, 'rank_geral']].copy()
    df_rank_geral.rename(columns={
        freq_col_name: 'frequencia_total' # Mantém 'Dezena' como está
    }, inplace=True)
    
    rank_geral_table_name = "rank_geral_dezenas_por_frequencia"
    try:
        db_manager.save_dataframe(df_rank_geral, rank_geral_table_name, if_exists='replace')
        logger.info(f"Ranking geral de dezenas salvo na tabela '{rank_geral_table_name}'. {len(df_rank_geral)} registros.")
    except Exception as e:
        logger.error(f"Erro ao salvar ranking geral na tabela '{rank_geral_table_name}': {e}", exc_info=True)
    
    logger.info("Cálculo e persistência de ranking geral de dezenas concluídos.")