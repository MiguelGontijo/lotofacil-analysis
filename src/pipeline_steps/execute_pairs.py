# src/pipeline_steps/execute_pairs.py

import argparse
from src.config import logger
from src.analysis.combination_analysis import calculate_combination_frequency

def execute_pair_analysis(args: argparse.Namespace):
    """ Executa e exibe a análise de pares. """
    logger.info(f"Executando análise de pares...")
    top_n = args.top_n
    max_c = args.max_concurso
    top_pairs = calculate_combination_frequency(2, top_n, max_c)
    if top_pairs:
        print(f"\n--- Top {top_n} Pares Mais Frequentes ---")
        for pair, count in top_pairs:
            print(f"Par: ({', '.join(map(str, pair))}) - Frequência: {count}")
    else:
         logger.warning("Não foram encontrados pares.")
    logger.info("Análise de pares concluída.")