# Exemplo de script de teste (ex: test_chunk.py na raiz)
import logging
from src.analysis.chunk_analysis import get_chunk_final_stats
from src.config import logger # Para ver os logs

logger.setLevel(logging.DEBUG) # Ver mais detalhes

if __name__ == "__main__":
    print("Testando get_chunk_final_stats para intervalo 10...")
    # Busca stats dos chunks de 10 até o último concurso disponível
    chunk_stats_10 = get_chunk_final_stats(interval_size=10)

    if chunk_stats_10 is not None:
        print(f"\nEncontrados {len(chunk_stats_10)} chunks completos de 10.")
        print("Últimos 5 chunks:")
        # Mostra as colunas de freq e rank para algumas dezenas
        cols_to_show = ['d1_freq', 'd1_rank', 'd10_freq', 'd10_rank', 'd25_freq', 'd25_rank']
        print(chunk_stats_10[cols_to_show].tail())
    else:
        print("Erro ao buscar stats dos chunks.")