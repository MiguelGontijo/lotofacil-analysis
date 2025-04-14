# src/pipeline_steps/execute_combinations.py

import argparse
from src.config import logger
from src.analysis.combination_analysis import calculate_combination_frequency

def execute_combination_analysis(args: argparse.Namespace):
    """ Executa e exibe a análise de combinações (Trios+). """
    logger.info(f"Executando análises de combinação (Trios+)...")
    top_n = args.top_n
    max_c = args.max_concurso

    for size in [3, 4, 5, 6]:
        combo_name_map = {3:"Trios", 4:"Quartetos", 5:"Quintetos", 6:"Sextetos"}
        combo_name = combo_name_map.get(size)
        logger.info(f"Calculando os {top_n} {combo_name} mais frequentes...")

        # Recebe a lista de resultados
        top_combos_list = calculate_combination_frequency(size, top_n, max_c)

        if top_combos_list:
            print(f"\n--- Top {top_n} {combo_name} Mais Frequentes ---")
            for combo, count in top_combos_list:
                print(f"Comb: ({', '.join(map(str, combo))}) - Freq: {count}")
        else:
            # A função de análise já loga warning
             logger.info(f"Nenhuma combinação encontrada para {combo_name} ou análise falhou.")
    logger.info("Análises de combinação (Trios+) concluídas.")