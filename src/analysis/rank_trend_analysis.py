# src/analysis/rank_trend_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any # Mantido por hábito, pode não ser usado diretamente aqui
import logging

# Importar constantes necessárias e existentes de config.py
from src.config import CHUNK_TYPES_CONFIG, ALL_NUMBERS # Usaremos CHUNK_TYPES_CONFIG para saber quais tabelas de frequência ler
from src.database_manager import DatabaseManager # Para interagir com o banco de dados

logger = logging.getLogger(__name__) # Logger específico para este módulo

def calculate_and_persist_rank_per_chunk(db_manager: DatabaseManager):
    """
    Lê as tabelas de frequência por chunk (evol_metric_frequency_...),
    calcula o ranking das dezenas dentro de cada chunk baseado na frequência,
    e salva os resultados em novas tabelas (evol_rank_frequency_bloco_...).

    Args:
        db_manager: Instância do DatabaseManager para ler e salvar dados.
    """
    logger.info("Iniciando cálculo e persistência de ranking de frequência por chunk.")

    for chunk_type, config in CHUNK_TYPES_CONFIG.items():
        chunk_sizes = config.get('sizes', [])
        
        for size in chunk_sizes:
            freq_table_name = f"evol_metric_frequency_{chunk_type}_{size}"
            rank_table_name = f"evol_rank_frequency_bloco_{chunk_type}_{size}" # Novo nome de tabela para os ranks
            
            logger.info(f"Processando ranking para chunks: tipo='{chunk_type}', tamanho={size}")
            logger.debug(f"Lendo da tabela de frequência: '{freq_table_name}', Salvando ranking em: '{rank_table_name}'")

            if not db_manager.table_exists(freq_table_name):
                logger.warning(f"Tabela de frequência '{freq_table_name}' não encontrada. Pulando cálculo de ranking para esta configuração.")
                continue

            df_freq_chunk = db_manager.load_dataframe_from_db(freq_table_name)

            if df_freq_chunk is None or df_freq_chunk.empty:
                logger.warning(f"DataFrame de frequência carregado de '{freq_table_name}' está vazio. Pulando cálculo de ranking.")
                continue
            
            # Colunas esperadas em df_freq_chunk:
            # chunk_seq_id, chunk_start_contest, chunk_end_contest, dezena, frequencia_absoluta
            required_cols = ['chunk_seq_id', 'dezena', 'frequencia_absoluta', 'chunk_start_contest', 'chunk_end_contest']
            if not all(col in df_freq_chunk.columns for col in required_cols):
                logger.error(f"Tabela '{freq_table_name}' não contém todas as colunas esperadas ({required_cols}). Colunas presentes: {df_freq_chunk.columns}. Pulando.")
                continue

            try:
                # Adiciona a coluna de rank ao DataFrame original para manter as outras informações
                # method='dense': ranks iguais para valores iguais, sem pular o próximo rank (ex: 1, 2, 2, 3).
                # ascending=False: maior frequência = menor rank (rank 1 é o melhor).
                df_freq_chunk['rank_no_bloco'] = df_freq_chunk.groupby('chunk_seq_id')['frequencia_absoluta'] \
                                                              .rank(method='dense', ascending=False).astype(int)
            except Exception as e:
                logger.error(f"Erro ao calcular o rank para a tabela '{freq_table_name}': {e}", exc_info=True)
                continue # Pula para a próxima configuração de chunk/size
                
            # Prepara o DataFrame para salvar, renomeando a coluna de frequência para clareza
            df_to_save = df_freq_chunk.rename(columns={'frequencia_absoluta': 'frequencia_no_bloco'})
            
            # Define a ordem desejada das colunas para a tabela de rank
            # Inclui todas as colunas originais da tabela de frequência mais a nova coluna de rank
            cols_order = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest', 
                          'dezena', 'frequencia_no_bloco', 'rank_no_bloco']
            
            # Garante que todas as colunas na ordem existam, preenchendo com None se alguma estiver faltando (improvável)
            for col in cols_order:
                if col not in df_to_save.columns:
                    df_to_save[col] = None 
            df_to_save = df_to_save[cols_order] # Reordena/seleciona as colunas

            try:
                db_manager.save_dataframe_to_db(df_to_save, rank_table_name, if_exists='replace')
                logger.info(f"Ranking de frequência por chunk salvo na tabela '{rank_table_name}'. {len(df_to_save)} registros.")
            except Exception as e:
                logger.error(f"Erro ao salvar dados de ranking na tabela '{rank_table_name}': {e}", exc_info=True)

    logger.info("Cálculo e persistência de ranking de frequência por chunk concluídos.")


def calculate_and_persist_general_rank(db_manager: DatabaseManager):
    """
    Calcula o ranking geral das dezenas baseado na frequência absoluta total
    (da tabela 'frequencia_absoluta') e salva em uma nova tabela.
    """
    logger.info("Iniciando cálculo e persistência de ranking geral de dezenas por frequência.")
    freq_abs_table_name = "frequencia_absoluta" 

    if not db_manager.table_exists(freq_abs_table_name):
        logger.warning(f"Tabela '{freq_abs_table_name}' não encontrada. Não é possível calcular rank geral.")
        return

    df_freq_abs = db_manager.load_dataframe_from_db(freq_abs_table_name)

    if df_freq_abs is None or df_freq_abs.empty:
        logger.warning(f"DataFrame da tabela '{freq_abs_table_name}' está vazio. Não é possível calcular rank geral.")
        return
    
    # Tenta identificar as colunas de dezena e frequência de forma flexível
    dezena_col_name = next((col for col in ['Dezena', 'dezena'] if col in df_freq_abs.columns), None)
    freq_col_name = next((col for col in ['Frequencia Absoluta', 'frequencia_absoluta'] if col in df_freq_abs.columns), None)

    if not dezena_col_name or not freq_col_name:
        logger.error(f"Tabela '{freq_abs_table_name}' não contém as colunas de dezena ou frequência esperadas. Colunas encontradas: {df_freq_abs.columns.tolist()}")
        return

    # Calcula o rank geral
    df_freq_abs['rank_geral'] = df_freq_abs[freq_col_name].rank(method='dense', ascending=False).astype(int)
    
    # Prepara o DataFrame para salvar
    df_rank_geral = df_freq_abs[[dezena_col_name, freq_col_name, 'rank_geral']].copy()
    df_rank_geral.rename(columns={
        dezena_col_name: 'Dezena', # Padroniza nome da coluna
        freq_col_name: 'frequencia_total' # Renomeia para clareza
    }, inplace=True)
    
    rank_geral_table_name = "rank_geral_dezenas_por_frequencia"
    try:
        db_manager.save_dataframe_to_db(df_rank_geral, rank_geral_table_name, if_exists='replace')
        logger.info(f"Ranking geral de dezenas salvo na tabela '{rank_geral_table_name}'. {len(df_rank_geral)} registros.")
    except Exception as e:
        logger.error(f"Erro ao salvar ranking geral na tabela '{rank_geral_table_name}': {e}", exc_info=True)
    
    logger.info("Cálculo e persistência de ranking geral de dezenas concluídos.")