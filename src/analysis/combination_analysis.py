# Lotofacil_Analysis/src/analysis/combination_analysis.py
import logging
import pandas as pd
from itertools import combinations
from collections import Counter
from typing import List, Dict, Any, Set, Tuple, Union # Adicionado Union para type hints

# Imports necessários para a nova funcionalidade
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori

logger = logging.getLogger(__name__)

class CombinationAnalyzer:
    def __init__(self, all_numbers: List[int]):
        """
        Inicializa o CombinationAnalyzer.

        Args:
            all_numbers: Uma lista de todos os números possíveis no jogo (ex: 1 a 25).
        """
        self.all_numbers = sorted(all_numbers)
        logger.debug("CombinationAnalyzer instanciado com %d números.", len(self.all_numbers))

    def analyze_pairs(
        self, 
        all_draws_df: pd.DataFrame, 
        drawn_numbers_col: str = 'drawn_numbers',
        contest_id_col: str = 'contest_id' # Nome da coluna de identificação do concurso
    ) -> pd.DataFrame:
        """
        Analisa os sorteios para calcular a frequência, última ocorrência e atraso de pares de dezenas.
        Calcula o atraso para todos os pares possíveis.

        Args:
            all_draws_df (pd.DataFrame): DataFrame com todos os sorteios.
                Deve conter as colunas especificadas por drawn_numbers_col e contest_id_col.
            drawn_numbers_col (str): Nome da coluna contendo as listas de dezenas sorteadas.
            contest_id_col (str): Nome da coluna contendo o ID do concurso.

        Returns:
            pd.DataFrame: DataFrame com as métricas dos pares:
                'pair_str' (str): O par formatado (ex: "1-2").
                'frequency' (int): Quantas vezes o par foi sorteado.
                'last_contest' (int): O ID do último concurso em que o par foi sorteado (0 se nunca).
                'current_delay' (int): Número de concursos desde a última ocorrência.
                                       Se nunca ocorreu, é o ID do último concurso no histórico.
        """
        logger.info("Iniciando análise de pares.")
        
        if drawn_numbers_col not in all_draws_df.columns:
            msg = f"Coluna '{drawn_numbers_col}' não encontrada no DataFrame de sorteios."
            logger.error(msg)
            raise ValueError(msg)
        if contest_id_col not in all_draws_df.columns:
            msg = f"Coluna '{contest_id_col}' não encontrada no DataFrame de sorteios."
            logger.error(msg)
            raise ValueError(msg)

        pair_counts: Counter[Tuple[int, int]] = Counter()
        last_occurrence_pair: Dict[Tuple[int, int], int] = {}
        
        # Garante que drawn_numbers seja uma lista de inteiros e IDs de concurso sejam inteiros
        draws_data = all_draws_df[[contest_id_col, drawn_numbers_col]].copy()
        try:
            draws_data[contest_id_col] = draws_data[contest_id_col].astype(int)
            draws_data[drawn_numbers_col] = draws_data[drawn_numbers_col].apply(
                lambda x: sorted([int(n) for n in x]) if isinstance(x, (list, set, tuple)) else []
            )
        except Exception as e:
            logger.error(f"Erro ao converter colunas para o tipo esperado: {e}")
            raise

        max_contest_id_in_history = draws_data[contest_id_col].max() if not draws_data.empty else 0

        for _, row in draws_data.iterrows():
            contest_id = row[contest_id_col]
            drawn_numbers = row[drawn_numbers_col]
            
            if len(drawn_numbers) < 2:
                continue

            for pair_tuple in combinations(drawn_numbers, 2):
                pair_counts[pair_tuple] += 1
                last_occurrence_pair[pair_tuple] = contest_id
        
        # Cria todos os pares possíveis com base em self.all_numbers
        all_possible_pairs = list(combinations(self.all_numbers, 2))
        
        results = []
        for pair_tuple in all_possible_pairs:
            pair_str = f"{pair_tuple[0]}-{pair_tuple[1]}"
            frequency = pair_counts.get(pair_tuple, 0)
            last_contest = last_occurrence_pair.get(pair_tuple, 0)
            
            if frequency > 0:
                current_delay = max_contest_id_in_history - last_contest
            else:
                current_delay = max_contest_id_in_history # Atraso é o total de concursos se nunca saiu
                                                          # ou desde o início do histórico.

            results.append({
                'pair_str': pair_str,
                'frequency': frequency,
                'last_contest': last_contest,
                'current_delay': current_delay
            })
            
        pairs_df = pd.DataFrame(results)
        pairs_df = pairs_df.sort_values(by=['frequency', 'current_delay'], ascending=[False, True]).reset_index(drop=True)
        
        logger.info(f"Análise de pares concluída. {len(pairs_df)} pares processados.")
        return pairs_df

    def analyze_frequent_itemsets(
        self, 
        all_draws_df: pd.DataFrame, 
        min_support: float, 
        min_len: int = 3, 
        max_len: int = 10,
        drawn_numbers_col: str = 'drawn_numbers'
    ) -> pd.DataFrame:
        """
        Analisa os sorteios para encontrar conjuntos frequentes de dezenas (itemsets)
        usando o algoritmo Apriori.

        Args:
            all_draws_df: DataFrame com todos os sorteios.
            min_support: O suporte mínimo para um itemset ser considerado frequente (0.0 a 1.0).
            min_len: O tamanho mínimo do itemset a ser retornado.
            max_len: O tamanho máximo do itemset a ser retornado.
            drawn_numbers_col: Nome da coluna em all_draws_df que contém as dezenas sorteadas.

        Returns:
            DataFrame com os itemsets frequentes, seu suporte, tamanho e contagem de frequência.
            Colunas: 'itemset_str', 'support', 'length', 'frequency_count'.
        """
        logger.info(f"Iniciando análise de itemsets frequentes com min_support={min_support}, min_len={min_len}, max_len={max_len}")

        if drawn_numbers_col not in all_draws_df.columns:
            msg = f"Coluna '{drawn_numbers_col}' não encontrada no DataFrame de sorteios."
            logger.error(msg)
            raise ValueError(msg)
        
        if all_draws_df.empty:
            logger.warning("DataFrame de sorteios está vazio. Retornando DataFrame de itemsets vazio.")
            return pd.DataFrame(columns=['itemset_str', 'support', 'length', 'frequency_count'])

        try:
            transactions = all_draws_df[drawn_numbers_col].apply(
                lambda x: [int(n) for n in x] if isinstance(x, (list, set, tuple)) and x else []
            ).tolist()
        except Exception as e:
            logger.error(f"Erro ao processar a coluna '{drawn_numbers_col}': {e}")
            raise

        if not any(transactions): # Verifica se todas as transações estão vazias
            logger.warning("Nenhuma dezena encontrada nas transações. Retornando DataFrame de itemsets vazio.")
            return pd.DataFrame(columns=['itemset_str', 'support', 'length', 'frequency_count'])

        te = TransactionEncoder()
        try:
            te_ary = te.fit_transform(transactions) # te.fit(transactions).transform(transactions)
        except ValueError as e:
            # Isso pode acontecer se `transactions` for uma lista de listas vazias,
            # embora o TransactionEncoder deva ser capaz de lidar com isso.
            logger.error(f"Erro no TransactionEncoder (possivelmente devido a todas as transações serem vazias ou dados inesperados): {e}")
            logger.error(f"Primeiras 5 transações: {transactions[:5]}") # Log para depuração
            return pd.DataFrame(columns=['itemset_str', 'support', 'length', 'frequency_count'])
            
        df_one_hot = pd.DataFrame(te_ary, columns=te.columns_)

        logger.debug(f"DataFrame one-hot criado com {df_one_hot.shape[0]} transações e {df_one_hot.shape[1]} itens (dezenas únicas).")

        if df_one_hot.empty:
            logger.warning("DataFrame one-hot está vazio após o TransactionEncoder. Retornando DataFrame de itemsets vazio.")
            return pd.DataFrame(columns=['itemset_str', 'support', 'length', 'frequency_count'])

        frequent_itemsets_df = apriori(df_one_hot, min_support=min_support, use_colnames=True, verbose=0)
        logger.debug(f"Apriori encontrou {len(frequent_itemsets_df)} itemsets frequentes antes da filtragem por tamanho.")

        if frequent_itemsets_df.empty:
            logger.info("Nenhum itemset frequente encontrado com o min_support fornecido.")
            return pd.DataFrame(columns=['itemset_str', 'support', 'length', 'frequency_count'])

        frequent_itemsets_df['length'] = frequent_itemsets_df['itemsets'].apply(lambda x: len(x))

        filtered_itemsets_df = frequent_itemsets_df[
            (frequent_itemsets_df['length'] >= min_len) &
            (frequent_itemsets_df['length'] <= max_len)
        ].copy()
        logger.debug(f"Encontrados {len(filtered_itemsets_df)} itemsets após filtragem por tamanho ({min_len}-{max_len}).")

        if filtered_itemsets_df.empty:
            logger.info(f"Nenhum itemset frequente encontrado com tamanho entre {min_len} e {max_len}.")
            return pd.DataFrame(columns=['itemset_str', 'support', 'length', 'frequency_count'])
            
        total_contests = len(all_draws_df)
        filtered_itemsets_df['frequency_count'] = (filtered_itemsets_df['support'] * total_contests).round().astype(int)

        def format_itemset(itemset_frozenset: frozenset) -> str:
            return "-".join(map(str, sorted([int(item) for item in itemset_frozenset])))

        filtered_itemsets_df['itemset_str'] = filtered_itemsets_df['itemsets'].apply(format_itemset)
        
        result_df = filtered_itemsets_df[['itemset_str', 'support', 'length', 'frequency_count']]
        result_df = result_df.sort_values(by=['length', 'support'], ascending=[True, False]).reset_index(drop=True)
        
        logger.info(f"Análise de itemsets frequentes concluída. Retornando {len(result_df)} itemsets.")
        return result_df