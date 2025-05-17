# Lotofacil_Analysis/src/analysis/combination_analysis.py
import logging
import pandas as pd
from itertools import combinations
from collections import Counter
from typing import List, Dict, Any, Set, Tuple, Union 

from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules

logger = logging.getLogger(__name__)

class CombinationAnalyzer:
    def __init__(self, all_numbers: List[int]):
        self.all_numbers = sorted(all_numbers)
        logger.debug("CombinationAnalyzer instanciado com %d números.", len(self.all_numbers))

    def _format_frozenset_to_str(self, itemset_frozenset: frozenset) -> str:
        return "-".join(map(str, sorted([int(item) for item in itemset_frozenset])))

    def analyze_pairs(
        self, 
        all_draws_df: pd.DataFrame, 
        drawn_numbers_col: str = 'drawn_numbers',
        contest_id_col: str = 'contest_id'
    ) -> pd.DataFrame:
        # ... (código existente do analyze_pairs, sem alterações) ...
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
        
        all_possible_pairs = list(combinations(self.all_numbers, 2))
        
        results = []
        for pair_tuple in all_possible_pairs:
            pair_str = f"{pair_tuple[0]}-{pair_tuple[1]}"
            frequency = pair_counts.get(pair_tuple, 0)
            last_contest = last_occurrence_pair.get(pair_tuple, 0)
            
            if frequency > 0:
                current_delay = max_contest_id_in_history - last_contest
            else:
                current_delay = max_contest_id_in_history 

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
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Analisa os sorteios para encontrar conjuntos frequentes de dezenas (itemsets)
        usando o algoritmo Apriori.

        Retorna dois DataFrames:
        1. df_for_db: Formatado para salvar no banco de dados (com 'itemset_str'),
                      filtrado por min_len e max_len.
        2. frequent_itemsets_raw_mlxtend: Contém 'itemsets' como frozensets e 'support',
                                         resultado direto do apriori, *antes* da filtragem
                                         por min_len/max_len, para ser usado na geração de regras.
        """
        logger.info(f"Iniciando análise de itemsets frequentes com min_support={min_support}, min_len={min_len}, max_len={max_len}")

        empty_cols_db = ['itemset_str', 'support', 'length', 'frequency_count']
        empty_cols_rules_lookup = ['support', 'itemsets'] # Colunas típicas do apriori

        if drawn_numbers_col not in all_draws_df.columns:
            msg = f"Coluna '{drawn_numbers_col}' não encontrada no DataFrame de sorteios."
            logger.error(msg)
            raise ValueError(msg)
        
        if all_draws_df.empty:
            logger.warning("DataFrame de sorteios está vazio. Retornando DataFrames de itemsets vazios.")
            return pd.DataFrame(columns=empty_cols_db), pd.DataFrame(columns=empty_cols_rules_lookup)

        try:
            transactions = all_draws_df[drawn_numbers_col].apply(
                lambda x: [int(n) for n in x] if isinstance(x, (list, set, tuple)) and x else []
            ).tolist()
        except Exception as e:
            logger.error(f"Erro ao processar a coluna '{drawn_numbers_col}': {e}")
            raise

        if not any(transactions):
            logger.warning("Nenhuma dezena encontrada nas transações. Retornando DataFrames de itemsets vazios.")
            return pd.DataFrame(columns=empty_cols_db), pd.DataFrame(columns=empty_cols_rules_lookup)

        te = TransactionEncoder()
        try:
            te_ary = te.fit_transform(transactions)
        except ValueError as e:
            logger.error(f"Erro no TransactionEncoder: {e}")
            logger.error(f"Primeiras 5 transações: {transactions[:5]}")
            return pd.DataFrame(columns=empty_cols_db), pd.DataFrame(columns=empty_cols_rules_lookup)
            
        df_one_hot = pd.DataFrame(te_ary, columns=te.columns_)
        logger.debug(f"DataFrame one-hot criado com {df_one_hot.shape[0]} transações e {df_one_hot.shape[1]} itens.")

        if df_one_hot.empty:
            logger.warning("DataFrame one-hot está vazio. Retornando DataFrames de itemsets vazios.")
            return pd.DataFrame(columns=empty_cols_db), pd.DataFrame(columns=empty_cols_rules_lookup)

        # DataFrame bruto do apriori - este será usado para gerar as regras
        frequent_itemsets_raw_mlxtend = apriori(df_one_hot, min_support=min_support, use_colnames=True, verbose=0)
        logger.debug(f"Apriori encontrou {len(frequent_itemsets_raw_mlxtend)} itemsets frequentes (bruto).")

        if frequent_itemsets_raw_mlxtend.empty:
            logger.info("Nenhum itemset frequente encontrado com o min_support fornecido (bruto).")
            return pd.DataFrame(columns=empty_cols_db), frequent_itemsets_raw_mlxtend.copy()

        # Prepara o DataFrame para o banco de dados (df_for_db)
        # Faz uma cópia para não alterar o DataFrame bruto que será usado para as regras
        df_to_filter_for_db = frequent_itemsets_raw_mlxtend.copy()
        df_to_filter_for_db['length'] = df_to_filter_for_db['itemsets'].apply(lambda x: len(x))

        filtered_itemsets_df = df_to_filter_for_db[
            (df_to_filter_for_db['length'] >= min_len) &
            (df_to_filter_for_db['length'] <= max_len)
        ].copy()
        logger.debug(f"Encontrados {len(filtered_itemsets_df)} itemsets para DB após filtragem por tamanho ({min_len}-{max_len}).")

        if filtered_itemsets_df.empty:
            logger.info(f"Nenhum itemset frequente encontrado com tamanho entre {min_len} e {max_len} para o formato de DB.")
            df_for_db = pd.DataFrame(columns=empty_cols_db)
        else:
            total_contests = len(all_draws_df)
            df_for_db = filtered_itemsets_df.copy()
            df_for_db['frequency_count'] = (df_for_db['support'] * total_contests).round().astype(int)
            df_for_db['itemset_str'] = df_for_db['itemsets'].apply(self._format_frozenset_to_str)
            
            df_for_db = df_for_db[['itemset_str', 'support', 'length', 'frequency_count']]
            df_for_db = df_for_db.sort_values(by=['length', 'support'], ascending=[True, False]).reset_index(drop=True)
        
        logger.info(f"Análise de itemsets frequentes concluída. Retornando {len(df_for_db)} itemsets para DB e {len(frequent_itemsets_raw_mlxtend)} itemsets brutos para regras.")
        
        return df_for_db, frequent_itemsets_raw_mlxtend # Retorna o formatado e o bruto


    def generate_association_rules(
        self,
        frequent_itemsets_mlxtend_df: pd.DataFrame, 
        metric: str = "confidence",
        min_threshold: float = 0.5,
        min_lift: float = 0.0
    ) -> pd.DataFrame:
        """
        Gera regras de associação a partir de um DataFrame de itemsets frequentes (formato mlxtend).
        """
        logger.info(f"Gerando regras de associação com métrica='{metric}', limiar={min_threshold}, min_lift={min_lift}")

        if frequent_itemsets_mlxtend_df.empty:
            logger.warning("DataFrame de itemsets frequentes (formato mlxtend) está vazio. Nenhuma regra será gerada.")
            return pd.DataFrame()
        
        if 'itemsets' not in frequent_itemsets_mlxtend_df.columns or 'support' not in frequent_itemsets_mlxtend_df.columns:
            logger.error("DataFrame de itemsets frequentes (formato mlxtend) não contém as colunas 'itemsets' ou 'support'.")
            return pd.DataFrame()

        try:
            # A função association_rules precisa dos itemsets que atendem ao min_support,
            # incluindo aqueles de tamanho menor que min_len, para calcular corretamente os suportes
            # dos antecedentes/consequentes.
            rules_df = association_rules(
                frequent_itemsets_mlxtend_df, # Usa o DataFrame bruto do apriori
                metric=metric, 
                min_threshold=min_threshold
            )
        except KeyError as ke:
             # Este erro pode acontecer se, mesmo passando o DF bruto do apriori, algum subconjunto necessário
             # para uma regra não tiver sido encontrado pelo apriori (talvez por um min_support muito alto para ele).
             # Ou se o DataFrame não estiver como esperado.
            logger.error(f"KeyError ao gerar regras de associação com mlxtend: {ke}", exc_info=True)
            logger.error(f"Primeiras linhas do DataFrame de itemsets passado para association_rules:\n{frequent_itemsets_mlxtend_df.head()}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro inesperado ao gerar regras de associação com mlxtend: {e}", exc_info=True)
            return pd.DataFrame()

        if rules_df.empty:
            logger.info(f"Nenhuma regra de associação encontrada com os critérios: métrica='{metric}', limiar={min_threshold}.")
            return rules_df
        
        logger.info(f"Geradas {len(rules_df)} regras antes da filtragem por lift.")

        if min_lift > 0.0: # Aplica filtro de lift se min_lift for maior que 0
            rules_df = rules_df[rules_df['lift'] >= min_lift].copy() # .copy() para evitar SettingWithCopyWarning
            logger.info(f"{len(rules_df)} regras restantes após filtro de lift >= {min_lift}.")

        if rules_df.empty:
            logger.info(f"Nenhuma regra restante após filtro de lift.")
            return rules_df

        rules_df['antecedents_str'] = rules_df['antecedents'].apply(self._format_frozenset_to_str)
        rules_df['consequents_str'] = rules_df['consequents'].apply(self._format_frozenset_to_str)
        
        cols_to_keep = [
            'antecedents_str', 'consequents_str', 
            'antecedent support', 'consequent support', 'support', 
            'confidence', 'lift', 'leverage', 'conviction'
        ]
        final_cols = [col for col in cols_to_keep if col in rules_df.columns]
        
        rules_df = rules_df[final_cols]
        rules_df = rules_df.sort_values(by=['lift', 'confidence', 'support'], ascending=[False, False, False]).reset_index(drop=True)

        logger.info(f"Geração de regras de associação concluída. {len(rules_df)} regras finais.")
        return rules_df