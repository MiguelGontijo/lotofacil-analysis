# src/database_manager.py

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional

# Importa configurações e o logger
from src.config import DATABASE_PATH, TABLE_NAME, logger, NEW_BALL_COLUMNS

def save_to_db(df: pd.DataFrame,
               table_name: str = TABLE_NAME,
               db_path: Path = DATABASE_PATH,
               if_exists: str = 'replace') -> bool:
    """
    Salva um DataFrame em uma tabela do banco de dados SQLite.

    Args:
        df (pd.DataFrame): O DataFrame a ser salvo.
        table_name (str): O nome da tabela no banco de dados.
        db_path (Path): O caminho para o arquivo do banco de dados SQLite.
        if_exists (str): Ação a ser tomada se a tabela já existir.
                         'replace': Apaga a tabela existente e cria uma nova.
                         'append': Adiciona os dados ao final da tabela existente.
                         'fail': Levanta um erro se a tabela existir (padrão do pandas).

    Returns:
        bool: True se a operação foi bem-sucedida, False caso contrário.
    """
    logger.info(f"Tentando salvar dados na tabela '{table_name}' em {db_path}...")
    logger.info(f"Modo 'if_exists' definido como: '{if_exists}'")

    if df is None or df.empty:
        logger.warning("DataFrame está vazio ou é None. Nada para salvar.")
        return False

    try:
        # Usar um gerenciador de contexto garante que a conexão seja fechada
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor() # Precisamos de um cursor para criar o índice

            # Salva o DataFrame na tabela especificada
            # index=False: Não salva o índice do DataFrame como uma coluna no BD
            df.to_sql(name=table_name,
                      con=conn,
                      if_exists=if_exists,
                      index=False)

            logger.info(f"{len(df)} registros salvos com sucesso na tabela '{table_name}'.")

            # Criar um índice na coluna 'concurso' para otimizar futuras consultas
            index_name = f"idx_{table_name}_concurso"
            sql_create_index = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (concurso);"
            cursor.execute(sql_create_index)
            logger.info(f"Índice '{index_name}' criado (ou já existente) na coluna 'concurso'.")

            # Commit é chamado automaticamente ao sair do bloco 'with' em caso de sucesso
        return True

    except sqlite3.Error as e:
        logger.error(f"Erro SQLite ao salvar dados ou criar índice: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado ao salvar dados no banco de dados: {e}")
        return False

def read_data_from_db(db_path: Path = DATABASE_PATH,
                      table_name: str = TABLE_NAME,
                      columns: Optional[List[str]] = None,
                      concurso_minimo: Optional[int] = None, # PARÂMETRO ADICIONADO
                      concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Lê dados da tabela de sorteios do banco de dados SQLite para um DataFrame.

    Args:
        db_path (Path): Caminho para o arquivo do banco de dados SQLite.
        table_name (str): Nome da tabela a ser lida.
        columns (Optional[List[str]]): Lista de colunas a serem selecionadas. Se None, seleciona todas (*).
        concurso_minimo (Optional[int]): Se fornecido, lê apenas os concursos >= a este valor.
        concurso_maximo (Optional[int]): Se fornecido, lê apenas os concursos <= a este valor.

    Returns:
        Optional[pd.DataFrame]: DataFrame com os dados lidos ou None em caso de erro.
    """
    log_msg = f"Tentando ler dados da tabela '{table_name}' em {db_path}"
    conditions = []
    params = []

    # Constrói a cláusula WHERE dinamicamente
    if concurso_minimo is not None:
        conditions.append("concurso >= ?")
        params.append(concurso_minimo)
        log_msg += f" a partir do concurso {concurso_minimo}"

    if concurso_maximo is not None:
        conditions.append("concurso <= ?")
        params.append(concurso_maximo)
        log_msg += f" até o concurso {concurso_maximo}"

    logger.info(log_msg)

    try:
        with sqlite3.connect(db_path) as conn:
            # Constrói a query SQL
            select_cols = '*' if columns is None else ', '.join(f'"{col}"' for col in columns)
            sql_query = f"SELECT {select_cols} FROM {table_name}"

            if conditions:
                sql_query += " WHERE " + " AND ".join(conditions) # Adiciona WHERE se houver condições

            sql_query += " ORDER BY concurso ASC;" # Ordenar sempre para consistência

            # Lê os dados usando pandas
            df = pd.read_sql_query(sql_query, conn, params=params)

            # Tentar converter tipos após leitura do SQL (read_sql pode não preservar Int64/datas perfeitamente)
            if not df.empty:
                 if 'data_sorteio' in df.columns:
                      # Usar format='%Y-%m-%d %H:%M:%S' se souber o formato exato do SQLite
                      df['data_sorteio'] = pd.to_datetime(df['data_sorteio'], errors='coerce')
                 for col in df.columns:
                     # Se for coluna de concurso ou bola, tenta converter para Int64
                     if col == 'concurso' or col in NEW_BALL_COLUMNS:
                         # Usar .astype(float).astype('Int64') pode ser mais robusto se houver floats inesperados
                         df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            logger.info(f"{len(df)} registros lidos com sucesso.")
            return df

    except sqlite3.OperationalError as e:
         logger.error(f"Erro SQLite ao ler dados: Tabela '{table_name}' existe? Erro: {e}")
         return None
    except Exception as e:
        logger.error(f"Erro inesperado ao ler dados do banco de dados: {e}")
        return None