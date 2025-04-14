# src/data_loader.py

import pandas as pd
from pathlib import Path
import logging

# Importa as configurações definidas no config.py
from src.config import (
    EXCEL_FILE_PATH,
    ORIGINAL_COLUMNS,
    COLUMN_MAPPING,
    INT_COLUMNS,
    DATE_COLUMNS,
    logger
)

def load_and_clean_data(file_path: Path = EXCEL_FILE_PATH) -> pd.DataFrame | None:
    """
    Carrega os dados da Lotofácil de um arquivo Excel, seleciona colunas relevantes,
    renomeia as colunas e ajusta os tipos de dados.

    Args:
        file_path (Path): O caminho para o arquivo Excel. Padrão usa EXCEL_FILE_PATH de config.py.

    Returns:
        pd.DataFrame | None: Um DataFrame do Pandas com os dados limpos e processados,
                             ou None se ocorrer um erro na leitura do arquivo.
    """
    logger.info(f"Iniciando carregamento do arquivo: {file_path}")

    try:
        df = pd.read_excel(file_path, usecols=ORIGINAL_COLUMNS)
        logger.info(f"Arquivo Excel lido com sucesso. {len(df)} registros encontrados.")

    except FileNotFoundError:
        logger.error(f"Erro: Arquivo não encontrado em {file_path}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao ler o arquivo Excel: {e}")
        return None

    # 1. Renomear colunas
    df.rename(columns=COLUMN_MAPPING, inplace=True)
    logger.debug(f"Colunas renomeadas para: {list(df.columns)}")

    # 2. Ajustar tipos de dados
    # Datas
    for col in DATE_COLUMNS:
        try:
            # *** CORREÇÃO APLICADA AQUI ***
            # Adicionado dayfirst=True para interpretar corretamente DD/MM/YYYY
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            logger.debug(f"Coluna '{col}' convertida para datetime (formato DD/MM esperado).")
        except Exception as e:
            logger.warning(f"Não foi possível converter a coluna '{col}' para datetime: {e}")

    # Inteiros
    for col in INT_COLUMNS:
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            logger.debug(f"Coluna '{col}' convertida para Int64.")
        except Exception as e:
            logger.warning(f"Não foi possível converter a coluna '{col}' para Int64: {e}")

    # 3. Verificar e tratar valores nulos (opcional, mas bom)
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    if total_nulls > 0:
        logger.warning(f"Encontrados {total_nulls} valores nulos após conversões:")
        logger.warning(f"\n{null_counts[null_counts > 0]}")
        if df['concurso'].isnull().any() or df['data_sorteio'].isnull().any():
             logger.error("Valores nulos encontrados em 'concurso' ou 'data_sorteio'. Isso pode indicar problemas nos dados!")

    # 4. Ordenar pelo concurso (boa prática)
    if 'concurso' in df.columns and not df['concurso'].isnull().any():
        df.sort_values(by='concurso', inplace=True)
        df.reset_index(drop=True, inplace=True)
        logger.debug("DataFrame ordenado por 'concurso'.")

    logger.info("Processo de carregamento e limpeza concluído.")
    return df