# src/backtester/runner.py

from typing import Callable, Optional, Set, Dict
import pandas as pd # Para Optional[pd.DataFrame] se necessário no futuro

from src.config import logger
# Importa função para buscar resultados reais
from src.database_manager import get_draw_numbers
# Importa função para avaliar acertos
from src.backtester.evaluator import evaluate_hits, summarize_results

# Define um tipo para a função da estratégia
# Recebe concurso_maximo (int), retorna Optional[Set[int]]
StrategyFunction = Callable[[int], Optional[Set[int]]]

def run_backtest(strategy_func: StrategyFunction,
                 strategy_name: str,
                 start_contest: int,
                 end_contest: int) -> Optional[Dict[int, int]]:
    """
    Executa o backtest de uma estratégia em um intervalo de concursos.

    Args:
        strategy_func (Callable): A função que implementa a estratégia.
                                  Deve aceitar 'concurso_maximo' (int) e
                                  retornar um Set[int] com 15 números ou None.
        strategy_name (str): Nome da estratégia para logs.
        start_contest (int): Concurso inicial para o backtest (inclusive).
        end_contest (int): Concurso final para o backtest (inclusive).

    Returns:
        Optional[Dict[int, int]]: Dicionário resumido com a contagem de acertos
                                   por faixa (11 a 15), ou None se falhar.
    """
    logger.info(f"Iniciando Backtest para Estratégia: '{strategy_name}'")
    logger.info(f"Período: Concursos {start_contest} a {end_contest}")

    if start_contest <= 1 or end_contest < start_contest:
        logger.error("Intervalo de concursos inválido para backtest.")
        return None

    results: Dict[int, int] = {} # Armazena {concurso: acertos}

    total_contests = end_contest - start_contest + 1
    contests_processed = 0

    for contest_to_play in range(start_contest, end_contest + 1):
        # 1. Obter análise necessária até o concurso ANTERIOR (N-1)
        concurso_analise = contest_to_play - 1

        # 2. Chamar a função da estratégia para obter os números
        logger.debug(f"Backtest [Conc. {contest_to_play}]: Aplicando estratégia (baseada em dados até {concurso_analise})...")
        chosen_numbers = strategy_func(concurso_analise)

        if chosen_numbers is None or len(chosen_numbers) != 15 :
            logger.error(f"Backtest [Conc. {contest_to_play}]: Estratégia falhou ou não retornou 15 números. Abortando parcial.")
            results[contest_to_play] = -1 # Marca como erro
            continue # Pula para o próximo concurso

        # 3. Obter os números realmente sorteados no concurso N
        logger.debug(f"Backtest [Conc. {contest_to_play}]: Buscando resultado real...")
        actual_numbers = get_draw_numbers(contest_to_play)

        if actual_numbers is None:
            logger.error(f"Backtest [Conc. {contest_to_play}]: Não foi possível obter o resultado real. Abortando parcial.")
            results[contest_to_play] = -1 # Marca como erro
            continue

        # 4. Avaliar os acertos
        hits = evaluate_hits(chosen_numbers, actual_numbers)
        results[contest_to_play] = hits
        logger.debug(f"Backtest [Conc. {contest_to_play}]: Escolhidos {sorted(list(chosen_numbers))}, Sorteados {sorted(list(actual_numbers))} => Acertos: {hits}")

        contests_processed += 1
        if contests_processed % 100 == 0: # Log de progresso
             logger.info(f"Backtest progresso: {contests_processed}/{total_contests} concursos processados.")


    logger.info(f"Backtest concluído para '{strategy_name}'. Processados: {contests_processed}/{total_contests}.")

    # 5. Sumarizar e retornar resultados
    summary = summarize_results(results)
    return summary