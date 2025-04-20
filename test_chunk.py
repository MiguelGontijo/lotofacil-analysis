# Exemplo de script de teste (ex: test_chunk.py na raiz)
import logging
from pathlib import Path
import sys
import pandas as pd
from src.analysis.chunk_analysis import get_chunk_final_stats
from src.config import logger, DATABASE_PATH

def validate_database():
    """Valida se o banco de dados existe antes de executar os testes."""
    if not Path(DATABASE_PATH).exists():
        logger.error(f"Banco de dados não encontrado em: {DATABASE_PATH}")
        return False
    return True

def run_chunk_test():
    """Executa o teste de análise de chunks com validações."""
    logger.setLevel(logging.DEBUG)
    
    if not validate_database():
        return False
    
    try:
        print("\nTestando get_chunk_final_stats para intervalo 10...")
        chunk_stats_10 = get_chunk_final_stats(interval_size=10)
        
        if chunk_stats_10 is None:
            logger.error("get_chunk_final_stats retornou None")
            return False
            
        if chunk_stats_10.empty:
            logger.error("Nenhum dado de chunk encontrado")
            return False
            
        print(f"\nEncontrados {len(chunk_stats_10)} chunks completos de 10.")
        print("\nÚltimos 5 chunks:")
        cols_to_show = ['d1_freq', 'd1_rank', 'd10_freq', 'd10_rank', 'd25_freq', 'd25_rank']
        print(chunk_stats_10[cols_to_show].tail())
        
        return True
        
    except Exception as e:
        logger.error(f"Erro durante execução do teste: {str(e)}")
        return False

if __name__ == "__main__":
    success = run_chunk_test()
    sys.exit(0 if success else 1)