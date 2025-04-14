# src/backtester/evaluator.py

from typing import Set, Dict
from collections import Counter
from src.config import logger

def evaluate_hits(chosen_numbers: Set[int], actual_numbers: Set[int]) -> int:
    """
    Compara o conjunto de números escolhidos com os números sorteados
    e retorna a quantidade de acertos.

    Args:
        chosen_numbers (Set[int]): Conjunto de números escolhidos pela estratégia.
        actual_numbers (Set[int]): Conjunto de números realmente sorteados.

    Returns:
        int: O número de acertos (interseção entre os dois conjuntos).
             Retorna -1 se um dos inputs for inválido.
    """
    if chosen_numbers is None or actual_numbers is None:
        logger.error("Erro na avaliação: conjunto de números inválido (None).")
        return -1 # Indica erro
    if not isinstance(chosen_numbers, set) or not isinstance(actual_numbers, set):
         logger.error(f"Erro na avaliação: input não é um set. Escolhidos: {type(chosen_numbers)}, Sorteados: {type(actual_numbers)}")
         return -1

    hits = len(chosen_numbers.intersection(actual_numbers))
    # logger.debug(f"Avaliação: Escolhidos {chosen_numbers}, Sorteados {actual_numbers}, Acertos: {hits}")
    return hits


def summarize_results(results: Dict[int, int]) -> Dict[int, int]:
    """
    Recebe um dicionário de resultados {concurso: acertos} e retorna
    um resumo da contagem de cada faixa de acertos (11 a 15).

    Args:
        results (Dict[int, int]): Dicionário com {concurso: numero_de_acertos}.

    Returns:
        Dict[int, int]: Dicionário com {numero_de_acertos: contagem_de_vezes}.
    """
    hit_counts = Counter(results.values()) # Conta quantas vezes cada número de acertos ocorreu

    # Filtra para manter apenas as faixas de premiação relevantes (11 a 15) e acertos >= 0
    summary = {hits: count for hits, count in hit_counts.items() if 11 <= hits <= 15}

    # Garante que todas as faixas de 11 a 15 existam no resumo, mesmo que com 0 ocorrências
    for i in range(11, 16):
        if i not in summary:
            summary[i] = 0

    # Opcional: incluir contagem de < 11 acertos ou erros (-1)
    summary['<11'] = sum(count for hits, count in hit_counts.items() if 0 <= hits < 11)
    summary['errors'] = hit_counts.get(-1, 0) # Conta quantos erros (-1) ocorreram

    logger.info(f"Resumo do Backtest: {summary}")
    return summary