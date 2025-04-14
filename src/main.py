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
from src.analysis.combination_analysis import calculate_combination_frequency
from src.analysis.cycle_analysis import identify_cycles
# Importa a nova função de análise de atrasos
from src.analysis.delay_analysis import calculate_current_delay


pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)


def run_pipeline(args: argparse.Namespace):
    """
    Executa o pipeline de análise da Lotofácil com base nos argumentos fornecidos.
    """
    logger.info("Iniciando a aplicação Lotofacil Analysis")

    # --- Bloco de Carga/Salvamento ---
    if args.reload:
        logger.info("Opção '--reload' ativa...")
        cleaned_data = load_and_clean_data()
        if cleaned_data is not None:
            success = save_to_db(df=cleaned_data, if_exists='replace')
            if not success:
                logger.error("Falha ao salvar os dados no banco de dados.")
                return
        else:
            logger.error("Não foi possível carregar os dados do Excel.")
            return
    else:
        logger.info("Opção '--reload' inativa...")
        test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
        if test_read is None or test_read.empty:
             logger.error(f"Não foi possível ler dados do BD. Execute com --reload ou verifique o BD.")
             return

    max_concurso = args.max_concurso

    # Determina quais análises rodar
    run_all = 'all' in args.analysis
    run_freq = run_all or 'freq' in args.analysis
    run_pair = run_all or 'pair' in args.analysis
    run_comb = run_all or 'comb' in args.analysis
    run_cycle = run_all or 'cycle' in args.analysis
    run_delay = run_all or 'delay' in args.analysis # Flag para atraso

    # --- Passo 3: Análises de Frequência ---
    if run_freq:
        logger.info(f"Iniciando análises de frequência (até concurso {max_concurso or 'último'})...")
        overall_freq = calculate_overall_frequency(concurso_maximo=max_concurso)
        if overall_freq is not None:
            print("\n--- Frequência Geral das Dezenas ---")
            print(overall_freq.to_string())

        try:
            window_sizes = [int(w.strip()) for w in args.windows.split(',') if w.strip()]
        except ValueError:
            logger.error(f"Formato inválido para --windows: '{args.windows}'.")
            window_sizes = []

        for window in window_sizes:
            window_freq = calculate_windowed_frequency(window_size=window, concurso_maximo=max_concurso)
            if window_freq is not None:
                print(f"\n--- Frequência nos Últimos {window} Concursos ---")
                print(window_freq.to_string())

        cumulative_hist = calculate_cumulative_frequency_history(concurso_maximo=max_concurso)
        if cumulative_hist is not None:
            print("\n--- Histórico de Frequência Acumulada (Últimos 5 Registros) ---")
            print(cumulative_hist.tail())
        logger.info("Análises de frequência concluídas.")


    # --- Passo 7: Análise de Pares ---
    if run_pair:
        logger.info(f"Iniciando análise de pares (até concurso {max_concurso or 'último'})...")
        top_n_pairs = args.top_n
        logger.info(f"Calculando os {top_n_pairs} Pares mais frequentes...")
        top_pairs_result = calculate_combination_frequency(
            combination_size=2, top_n=top_n_pairs, concurso_maximo=max_concurso
        )
        if top_pairs_result:
            print(f"\n--- Top {top_n_pairs} Pares Mais Frequentes ---")
            for pair, count in top_pairs_result:
                pair_str = ", ".join(map(str, pair))
                print(f"Par: ({pair_str}) - Frequência: {count}")
        else: logger.warning(f"Não foram encontrados pares.")
        logger.info("Análise de pares concluída.")


    # --- Passo 4: Análises de Combinação (Trios em diante) ---
    if run_comb:
        logger.info(f"Iniciando análises de combinação (Trios+) (até concurso {max_concurso or 'último'})...")
        top_n_comb = args.top_n
        for size in [3, 4, 5, 6]:
             combo_name = {3: "Trios", 4: "Quartetos", 5: "Quintetos", 6: "Sextetos"}.get(size)
             logger.info(f"Calculando os {top_n_comb} {combo_name} mais frequentes...")
             top_combos = calculate_combination_frequency(
                 combination_size=size, top_n=top_n_comb, concurso_maximo=max_concurso
             )
             if top_combos:
                 print(f"\n--- Top {top_n_comb} {combo_name} Mais Frequentes ---")
                 for combo, count in top_combos:
                     combo_str = ", ".join(map(str, combo))
                     print(f"Combinação: ({combo_str}) - Frequência: {count}")
             else: logger.warning(f"Não foram encontradas combinações para {combo_name}.")
        logger.info("Análises de combinação (Trios+) concluídas.")


    # --- Passo 5: Análise de Ciclos ---
    if run_cycle:
        logger.info(f"Iniciando análise de ciclos (até concurso {max_concurso or 'último'})...")
        cycles_summary = identify_cycles()
        if cycles_summary is not None and not cycles_summary.empty:
             if max_concurso:
                 cycles_summary_filtered = cycles_summary[cycles_summary['concurso_fim'] <= max_concurso].copy()
             else:
                 cycles_summary_filtered = cycles_summary

             if not cycles_summary_filtered.empty:
                print("\n--- Resumo dos Ciclos Completos da Lotofácil ---")
                print(cycles_summary_filtered.to_string(index=False))
                avg_duration = cycles_summary_filtered['duracao'].mean()
                min_duration = cycles_summary_filtered['duracao'].min()
                max_duration = cycles_summary_filtered['duracao'].max()
                print(f"\nEstatísticas dos Ciclos (considerando até concurso {max_concurso or 'último'}):")
                print(f"- Número de ciclos completos: {len(cycles_summary_filtered)}")
                print(f"- Duração Média: {avg_duration:.2f} concursos")
                print(f"- Ciclo Mais Curto: {min_duration} concursos")
                print(f"- Ciclo Mais Longo: {max_duration} concursos")
             else:
                 logger.info(f"Nenhum ciclo completo encontrado até o concurso {max_concurso}.")

        elif cycles_summary is not None and cycles_summary.empty:
             logger.info("Nenhum ciclo completo foi encontrado nos dados.")
        else:
            logger.error("Falha ao executar a análise de ciclos.")
        logger.info("Análise de ciclos concluída.")


    # *** BLOCO ADICIONADO AQUI ***
    # --- Passo 8: Análise de Atrasos ---
    if run_delay:
        logger.info(f"Iniciando análise de atraso atual (até concurso {max_concurso or 'último'})...")
        current_delays = calculate_current_delay(concurso_maximo=max_concurso)

        if current_delays is not None:
            print("\n--- Atraso Atual das Dezenas ---")
            # Atraso 0 = saiu no último concurso analisado
            # Atraso 1 = não saiu no último, mas saiu no penúltimo, etc.
            print(current_delays.to_string())
        else:
            logger.error("Falha ao calcular o atraso atual das dezenas.")
        logger.info("Análise de atraso atual concluída.")


    logger.info("Aplicação Lotofacil Analysis finalizada.")


if __name__ == "__main__":
    # Configuração do argparse
    parser = argparse.ArgumentParser(
        description="Analisa dados históricos da Lotofácil.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Mostra os valores padrão na ajuda
        )

    parser.add_argument(
        '--reload',
        action='store_true',
        help="Força o recarregamento dos dados a partir do arquivo Excel, substituindo os dados no banco."
    )
    parser.add_argument(
        '--max-concurso',
        type=int,
        default=None, # Padrão é None (analisa tudo)
        metavar='NUMERO',
        help="Número máximo do concurso a ser considerado nas análises."
    )
    parser.add_argument(
        '--analysis',
        nargs='+',
        choices=['freq', 'pair', 'comb', 'cycle', 'delay', 'all'], # Inclui 'delay'
        default=['all'], # Padrão é 'all'
        metavar='TIPO',
        help="Análises a executar (freq, pair, comb, cycle, delay, all)."
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=15, # Padrão é 15
        metavar='N',
        help="Número de resultados a exibir nas listas de top pares/combinações."
    )
    parser.add_argument(
        '--windows',
        type=str,
        default='10,25,50', # Padrão é '10,25,50'
        metavar='W1,W2,...',
        help="Tamanhos das janelas para análise de frequência, separados por vírgula."
    )

    # Parseia os argumentos fornecidos na linha de comando
    args = parser.parse_args()

    # Chama a função principal passando os argumentos parseados
    run_pipeline(args)