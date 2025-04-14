# src/main.py

import logging
import argparse
import pandas as pd
from src.config import logger, TABLE_NAME, DATABASE_PATH
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db
from src.analysis.frequency_analysis import (
    calculate_overall_frequency,
    calculate_windowed_frequency,
    calculate_cumulative_frequency_history
)
# A função de combinação já faz o trabalho pesado para N=2
from src.analysis.combination_analysis import calculate_combination_frequency
from src.analysis.cycle_analysis import identify_cycles

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)


def run_pipeline(args: argparse.Namespace):
    """
    Executa o pipeline de análise da Lotofácil com base nos argumentos fornecidos.
    """
    logger.info("Iniciando a aplicação Lotofacil Analysis")

    # --- Bloco de Carga/Salvamento (igual ao anterior) ---
    if args.reload:
        logger.info("Opção '--reload' ativa...") # Mensagens omitidas
        cleaned_data = load_and_clean_data()
        if cleaned_data is not None:
            success = save_to_db(df=cleaned_data, if_exists='replace')
            if not success: return
        else: return
    else:
        logger.info("Opção '--reload' inativa...") # Mensagens omitidas
        test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
        if test_read is None or test_read.empty: return

    max_concurso = args.max_concurso

    # Determina quais análises rodar
    run_all = 'all' in args.analysis
    run_freq = run_all or 'freq' in args.analysis
    run_pair = run_all or 'pair' in args.analysis # Nova flag para pares
    run_comb = run_all or 'comb' in args.analysis
    run_cycle = run_all or 'cycle' in args.analysis


    # --- Passo 3: Análises de Frequência ---
    if run_freq:
        logger.info(f"Iniciando análises de frequência (até concurso {max_concurso or 'último'})...")
        # (Código da análise de frequência omitido - igual ao anterior)
        # ...
        logger.info("Análises de frequência concluídas.")


    # --- Passo 7: Análise de Pares ---
    if run_pair:
        logger.info(f"Iniciando análise de pares (até concurso {max_concurso or 'último'})...")
        top_n_pairs = args.top_n # Usamos o mesmo argumento --top-n
        logger.info(f"Calculando os {top_n_pairs} Pares mais frequentes...")

        # Reutilizamos a função de combinação com combination_size=2
        top_pairs_result = calculate_combination_frequency(
            combination_size=2,
            top_n=top_n_pairs,
            concurso_maximo=max_concurso
        )

        if top_pairs_result:
            print(f"\n--- Top {top_n_pairs} Pares Mais Frequentes ---")
            for pair, count in top_pairs_result:
                pair_str = ", ".join(map(str, pair))
                print(f"Par: ({pair_str}) - Frequência: {count}")
        else:
            logger.warning(f"Não foram encontrados pares.")
        logger.info("Análise de pares concluída.")


    # --- Passo 4: Análises de Combinação (Trios em diante) ---
    if run_comb:
        logger.info(f"Iniciando análises de combinação (Trios+) (até concurso {max_concurso or 'último'})...")
        top_n_comb = args.top_n

        # Começamos do 3 agora, pois o 2 (pares) já foi tratado
        for size in [3, 4, 5, 6]:
             combo_name = {3: "Trios", 4: "Quartetos", 5: "Quintetos", 6: "Sextetos"}.get(size)
             logger.info(f"Calculando os {top_n_comb} {combo_name} mais frequentes...")
             # (Código da chamada e impressão igual ao anterior)
             top_combos = calculate_combination_frequency(
                 combination_size=size, top_n=top_n_comb, concurso_maximo=max_concurso
             )
             if top_combos:
                 print(f"\n--- Top {top_n_comb} {combo_name} Mais Frequentes ---")
                 for combo, count in top_combos:
                     combo_str = ", ".join(map(str, combo))
                     print(f"Combinação: ({combo_str}) - Frequência: {count}")
             else: logger.warning(f"...") # Mensagem igual anterior
        logger.info("Análises de combinação (Trios+) concluídas.")


    # --- Passo 5: Análise de Ciclos ---
    if run_cycle:
        logger.info(f"Iniciando análise de ciclos (até concurso {max_concurso or 'último'})...")
        # (Código da análise de ciclos omitido - igual ao anterior)
        # ...
        logger.info("Análise de ciclos concluída.")


    logger.info("Aplicação Lotofacil Analysis finalizada.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analisa dados históricos da Lotofácil.")

    parser.add_argument('--reload', action='store_true', help="Força recarga do Excel.")
    parser.add_argument('--max-concurso', type=int, default=None, metavar='NUMERO', help="Concurso máximo para análise.")
    parser.add_argument(
        '--analysis',
        nargs='+',
        # Adiciona 'pair' às opções
        choices=['freq', 'pair', 'comb', 'cycle', 'all'],
        default=['all'],
        metavar='TIPO',
        help="Análises a executar (freq, pair, comb, cycle, all). Padrão: all."
    )
    parser.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N resultados para pares/combinações. Padrão: 15.")
    parser.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas de frequência. Padrão: 10,25,50.")

    args = parser.parse_args()
    run_pipeline(args)