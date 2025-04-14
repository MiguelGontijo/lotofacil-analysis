# src/main.py

import logging
import argparse # Importa o módulo para argumentos de linha de comando
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

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)


# A função principal agora recebe os argumentos parseados
def run_pipeline(args: argparse.Namespace):
    """
    Executa o pipeline de análise da Lotofácil com base nos argumentos fornecidos.
    """
    logger.info("Iniciando a aplicação Lotofacil Analysis")

    # --- Bloco de Carga/Salvamento ---
    if args.reload: # Usa o argumento --reload
        logger.info("Opção '--reload' ativa. Recarregando do Excel e salvando no BD.")
        cleaned_data = load_and_clean_data()
        if cleaned_data is not None:
            logger.info("Dados carregados e limpos com sucesso do Excel.")
            success = save_to_db(df=cleaned_data, if_exists='replace')
            if not success:
                logger.error("Falha ao salvar os dados no banco de dados.")
                return
        else:
            logger.error("Não foi possível carregar os dados do Excel.")
            return
    else:
        logger.info("Opção '--reload' inativa. Assumindo que os dados estão no BD.")
        test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
        if test_read is None or test_read.empty:
             logger.error(f"Não foi possível ler dados do BD. Execute com --reload ou verifique o BD.")
             return

    # Define o concurso máximo para todas as análises
    max_concurso = args.max_concurso # Usa o argumento --max-concurso

    # Determina quais análises rodar
    run_all = 'all' in args.analysis
    run_freq = run_all or 'freq' in args.analysis
    run_comb = run_all or 'comb' in args.analysis
    run_cycle = run_all or 'cycle' in args.analysis

    # --- Passo 3: Análises de Frequência ---
    if run_freq:
        logger.info(f"Iniciando análises de frequência (até concurso {max_concurso or 'último'})...")
        overall_freq = calculate_overall_frequency(concurso_maximo=max_concurso)
        if overall_freq is not None:
            print("\n--- Frequência Geral das Dezenas ---")
            print(overall_freq.to_string())

        # Processa as janelas especificadas
        try:
            window_sizes = [int(w.strip()) for w in args.windows.split(',') if w.strip()]
        except ValueError:
            logger.error(f"Formato inválido para --windows: '{args.windows}'. Use números separados por vírgula (ex: 10,25,50).")
            window_sizes = [] # Define como vazio para não executar o loop

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

    # --- Passo 4: Análises de Combinação ---
    if run_comb:
        logger.info(f"Iniciando análises de combinação (até concurso {max_concurso or 'último'})...")
        top_n_comb = args.top_n # Usa o argumento --top-n

        for size in [3, 4, 5, 6]: # Trios, Quartetos, Quintetos, Sextetos
             combo_name = {3: "Trios", 4: "Quartetos", 5: "Quintetos", 6: "Sextetos"}.get(size)
             logger.info(f"Calculando os {top_n_comb} {combo_name} mais frequentes...")
             top_combos = calculate_combination_frequency(
                 combination_size=size,
                 top_n=top_n_comb,
                 concurso_maximo=max_concurso
             )

             if top_combos:
                 print(f"\n--- Top {top_n_comb} {combo_name} Mais Frequentes ---")
                 for combo, count in top_combos:
                     combo_str = ", ".join(map(str, combo))
                     print(f"Combinação: ({combo_str}) - Frequência: {count}")
             else:
                 logger.warning(f"Não foram encontradas combinações para {combo_name}.")
        logger.info("Análises de combinação concluídas.")

    # --- Passo 5: Análise de Ciclos ---
    if run_cycle:
        # Nota: Análise de ciclos geralmente faz mais sentido com todos os dados,
        # mas respeitamos o max_concurso se fornecido (identificará ciclos até aquele ponto)
        logger.info(f"Iniciando análise de ciclos (até concurso {max_concurso or 'último'})...")
        cycles_summary = identify_cycles() # identify_cycles já lê os dados necessários

        if cycles_summary is not None and not cycles_summary.empty:
             # Filtra os ciclos para incluir apenas aqueles que terminam até max_concurso, se definido
             if max_concurso:
                 cycles_summary = cycles_summary[cycles_summary['concurso_fim'] <= max_concurso].copy()

             if not cycles_summary.empty:
                print("\n--- Resumo dos Ciclos Completos da Lotofácil ---")
                print(cycles_summary.to_string(index=False))

                avg_duration = cycles_summary['duracao'].mean()
                min_duration = cycles_summary['duracao'].min()
                max_duration = cycles_summary['duracao'].max()
                print(f"\nEstatísticas dos Ciclos (considerando até concurso {max_concurso or 'último'}):")
                print(f"- Número de ciclos completos: {len(cycles_summary)}")
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


    logger.info("Aplicação Lotofacil Analysis finalizada.")


if __name__ == "__main__":
    # Configuração do argparse
    parser = argparse.ArgumentParser(description="Analisa dados históricos da Lotofácil.")

    parser.add_argument(
        '--reload',
        action='store_true', # Torna isso uma flag booleana (presente=True, ausente=False)
        help="Força o recarregamento dos dados a partir do arquivo Excel, substituindo os dados no banco."
    )
    parser.add_argument(
        '--max-concurso',
        type=int,
        default=None, # Nenhum por padrão significa usar todos os dados
        metavar='NUMERO', # Nome do valor esperado na ajuda
        help="Número máximo do concurso a ser considerado nas análises."
    )
    parser.add_argument(
        '--analysis',
        nargs='+', # Aceita um ou mais valores
        choices=['freq', 'comb', 'cycle', 'all'], # Opções válidas
        default=['all'], # Padrão é rodar todas as análises
        metavar='TIPO',
        help="Especifica quais análises executar (freq, comb, cycle, all). Padrão: all."
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=15, # Mesmo padrão de antes
        metavar='N',
        help="Número de resultados a exibir nas listas de top combinações. Padrão: 15."
    )
    parser.add_argument(
        '--windows',
        type=str,
        default='10,25,50', # Mesmo padrão de antes
        metavar='W1,W2,...',
        help="Tamanhos das janelas para análise de frequência, separados por vírgula. Padrão: 10,25,50."
    )

    # Parseia os argumentos fornecidos na linha de comando
    args = parser.parse_args()

    # Chama a função principal passando os argumentos parseados
    run_pipeline(args)