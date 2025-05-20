# src/strategies/combination_properties_strategy.py
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from itertools import combinations
import math # Para math.comb

# Supondo que BaseStrategy e os componentes do Aggregator/DBManager são importáveis
# Ajuste os caminhos de importação conforme a estrutura do seu projeto.
from .base_strategy import BaseStrategy
from ..database_manager import DatabaseManager
from ..analysis_aggregator import AnalysisAggregator
# from ..config import config as app_config

class CombinationAndPropertiesStrategy(BaseStrategy):
    """
    Estratégia que pontua dezenas com base na sua participação em itemsets fortes
    e, em seguida, seleciona o conjunto final de 15 dezenas que melhor
    se adequa a propriedades globais de jogo desejadas.
    Utiliza o AnalysisAggregator para buscar dados de itemsets.
    """

    def __init__(self,
                 db_manager: DatabaseManager,
                 config: Dict[str, Any], # Config global do app
                 analysis_aggregator: AnalysisAggregator,
                 # Parâmetros específicos da estratégia:
                 itemset_k_values: Optional[List[int]] = None, # Default [2,3] se None, ou o que o Aggregator suportar
                 itemset_min_support: Optional[float] = None,
                 itemset_min_lift: Optional[float] = None,
                 itemset_metric_to_use_as_score: str = 'itemset_score', # Coluna do Aggregator para score do itemset
                 # Parâmetros para Fase 2: Propriedades Globais (permanecem na estratégia)
                 target_sum_range: Tuple[int, int] = (180, 220),
                 target_even_count_range: Tuple[int, int] = (6, 9), # Implica 15 - count para ímpares
                 # Adicionar outros targets de propriedades (primos, linhas, colunas) conforme necessário
                 # via strategy_params e processados no __init__ ou _score_combination_properties
                 candidate_pool_size: int = 30, # Top N dezenas da fase de scoring para gerar combinações
                 max_combinations_to_evaluate: int = 50000 # Limite para avaliação de combinações
                 ):
        # Passando todos os params para BaseStrategy para que fiquem em self.strategy_specific_params
        super().__init__(db_manager, config, analysis_aggregator,
                         itemset_k_values=itemset_k_values if itemset_k_values is not None else [2,3],
                         itemset_min_support=itemset_min_support,
                         itemset_min_lift=itemset_min_lift,
                         itemset_metric_to_use_as_score=itemset_metric_to_use_as_score,
                         target_sum_range=target_sum_range,
                         target_even_count_range=target_even_count_range,
                         candidate_pool_size=candidate_pool_size,
                         max_combinations_to_evaluate=max_combinations_to_evaluate)

        # Atribuindo os parâmetros à instância para fácil acesso
        self.itemset_k_values = self.strategy_specific_params.get('itemset_k_values')
        self.itemset_min_support = self.strategy_specific_params.get('itemset_min_support')
        self.itemset_min_lift = self.strategy_specific_params.get('itemset_min_lift')
        self.itemset_metric_to_use_as_score = self.strategy_specific_params.get('itemset_metric_to_use_as_score')
        
        self.target_sum_range = self.strategy_specific_params.get('target_sum_range')
        self.target_even_count_range = self.strategy_specific_params.get('target_even_count_range')
        self.candidate_pool_size = self.strategy_specific_params.get('candidate_pool_size')
        self.max_combinations_to_evaluate = self.strategy_specific_params.get('max_combinations_to_evaluate')
        
        # Cache para dados de itemsets, se necessário, embora cada chamada possa ser única
        # self._itemset_data_cache: Dict[str, pd.DataFrame] = {}

    def get_name(self) -> str:
        k_values_str = str(self.itemset_k_values).replace(" ","")
        return (f"CombinationAndPropertiesStrategy(k={k_values_str}, "
                f"sup>={self.itemset_min_support}, lift>={self.itemset_min_lift}, "
                f"pool={self.candidate_pool_size})")

    def get_description(self) -> str:
        return ("Pontua dezenas por participação em itemsets fortes (via AnalysisAggregator) e seleciona "
                "o jogo final baseado em propriedades globais (soma, pares/ímpares, etc.) definidas na estratégia.")

    def _get_strong_itemsets_data(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Busca dados de itemsets (pares, trios, etc.) que atendem a critérios de "força",
        utilizando o método get_itemset_analysis_data do AnalysisAggregator.
        """
        # print(f"INFO (Strategy:{self.get_name()}): Buscando dados de itemsets fortes via Aggregator (até concurso {latest_draw_id})...")
        
        df_itemsets = self.analysis_aggregator.get_itemset_analysis_data(
            latest_concurso_id=latest_draw_id,
            k_values=self.itemset_k_values,
            min_support=self.itemset_min_support,
            min_lift=self.itemset_min_lift
        )

        if df_itemsets is None or df_itemsets.empty:
            print(f"AVISO ({self.get_name()}): Nenhum itemset forte encontrado pelos critérios definidos via Aggregator.")
            return pd.DataFrame(columns=['itemset', 'metric_value', 'k'])

        # A coluna 'itemset_metric_to_use_as_score' (ex: 'itemset_score', 'support', 'lift')
        # deve ser usada como a métrica de força do itemset.
        if self.itemset_metric_to_use_as_score not in df_itemsets.columns:
            print(f"ERRO ({self.get_name()}): Coluna '{self.itemset_metric_to_use_as_score}' "
                  "não encontrada nos dados de itemset do Aggregator. Verifique o parâmetro "
                  "'itemset_metric_to_use_as_score' da estratégia e a saída do Aggregator.")
            # Adiciona uma coluna de score neutro para evitar falhas, mas idealmente isso não deveria acontecer.
            df_itemsets['metric_value'] = 0.0
        else:
            df_itemsets = df_itemsets.rename(columns={self.itemset_metric_to_use_as_score: 'metric_value'})
        
        # Garantir que a coluna 'itemset' contenha tuplas (o aggregator deve cuidar disso)
        # Mas uma verificação/conversão defensiva pode ser útil.
        if 'itemset' in df_itemsets.columns:
            df_itemsets['itemset'] = df_itemsets['itemset'].apply(
                lambda x: tuple(sorted(x)) if isinstance(x, (list, pd.Series, set)) and not isinstance(x, tuple) else x
            )
        else:
             print(f"ERRO ({self.get_name()}): Coluna 'itemset' não encontrada nos dados do Aggregator.")
             return pd.DataFrame(columns=['itemset', 'metric_value', 'k'])


        # Garantir que a coluna 'k' (tamanho do itemset) exista, se não vier do aggregator, inferir.
        if 'k' not in df_itemsets.columns and 'itemset' in df_itemsets.columns:
            df_itemsets['k'] = df_itemsets['itemset'].apply(len)
        elif 'k' not in df_itemsets.columns:
             print(f"ERRO ({self.get_name()}): Coluna 'k' não encontrada e não pode ser inferida.")
             return pd.DataFrame(columns=['itemset', 'metric_value', 'k'])

        return df_itemsets[['itemset', 'metric_value', 'k']]


    def generate_scores(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Fase 1: Pontua dezenas individualmente com base na sua participação em itemsets fortes.
        """
        df_strong_itemsets = self._get_strong_itemsets_data(latest_draw_id)
        
        # _all_dezenas_list é herdado da BaseStrategy
        base_dezenas_df = pd.DataFrame({'dezena': self._all_dezenas_list})

        if df_strong_itemsets.empty:
            df_scores = base_dezenas_df.copy()
            df_scores['raw_itemset_score'] = 0.0
        else:
            dezena_scores_accumulation: Dict[int, float] = {d: 0.0 for d in self._all_dezenas_list}
            for _, row in df_strong_itemsets.iterrows():
                itemset = row.get('itemset') # Usar .get() para segurança
                metric = row.get('metric_value', 0.0) # Default para 0 se a coluna não existir por algum motivo
                
                if not isinstance(itemset, tuple):
                    # print(f"AVISO ({self.get_name()}): Itemset não é uma tupla: {itemset} (tipo: {type(itemset)}). Pulando.")
                    continue
                for dezena in itemset:
                    if dezena in dezena_scores_accumulation: # Segurança adicional
                        dezena_scores_accumulation[dezena] += metric
                    # else:
                        # print(f"AVISO ({self.get_name()}): Dezena {dezena} do itemset {itemset} não está na lista de dezenas válidas.")


            df_scores = pd.DataFrame(list(dezena_scores_accumulation.items()), columns=['dezena', 'raw_itemset_score'])

        # Garantir que todas as dezenas estejam presentes, mesmo que com score 0
        df_scores = pd.merge(base_dezenas_df, df_scores, on='dezena', how='left').fillna({'raw_itemset_score': 0.0})

        # Normalizar o 'raw_itemset_score'
        scaler = MinMaxScaler()
        if 'raw_itemset_score' in df_scores.columns and df_scores['raw_itemset_score'].nunique() > 1:
            df_scores['score'] = scaler.fit_transform(df_scores[['raw_itemset_score']])
        elif 'raw_itemset_score' in df_scores.columns: # Todos os scores são iguais
            df_scores['score'] = 0.5 # Valor neutro
        else: # Coluna não existe (improvável devido ao fillna, mas defensivo)
            df_scores['score'] = 0.0
        
        df_final_scores = df_scores.sort_values(by='score', ascending=False).reset_index(drop=True)
        df_final_scores['ranking_strategy'] = df_final_scores.index + 1
        
        return df_final_scores[['dezena', 'score', 'ranking_strategy']]

    def _score_combination_properties(self, combination: List[int]) -> float:
        """
        Calcula um score para uma combinação de 15 dezenas com base
        no seu alinhamento com as propriedades de jogo desejadas.
        (Lógica interna da estratégia, não depende diretamente do Aggregator para ESTA avaliação).
        """
        if len(combination) != 15: return 0.0

        score = 0.0
        num_properties_checked = 0

        # 1. Soma das dezenas
        soma = sum(combination)
        num_properties_checked += 1
        if self.target_sum_range[0] <= soma <= self.target_sum_range[1]:
            score += 1.0
        
        # 2. Contagem de Pares
        even_count = sum(1 for d in combination if d % 2 == 0)
        num_properties_checked += 1
        if self.target_even_count_range[0] <= even_count <= self.target_even_count_range[1]:
            score += 1.0
        
        # Adicionar aqui a verificação de outras propriedades desejadas (primos, etc.)
        # Exemplo: Contagem de Primos (lista de primos até 25: 2,3,5,7,11,13,17,19,23)
        # primos_lotofacil = {2, 3, 5, 7, 11, 13, 17, 19, 23}
        # target_prime_count_range = self.strategy_specific_params.get('target_prime_count_range', (3, 6)) # Exemplo
        # if target_prime_count_range:
        #     prime_count = sum(1 for d in combination if d in primos_lotofacil)
        #     num_properties_checked += 1
        #     if target_prime_count_range[0] <= prime_count <= target_prime_count_range[1]:
        #         score += 1.0

        return score / num_properties_checked if num_properties_checked > 0 else 0.0

    def select_numbers(self,
                       scores_df: pd.DataFrame, 
                       num_to_select: int = 15,
                       selection_params: Optional[Dict[str, Any]] = None) -> List[int]:
        """
        Fase 2: Seleciona o conjunto final de N dezenas (idealmente 15 para esta estratégia).
        Gera combinações a partir de um pool de candidatas e escolhe a melhor
        com base nas propriedades globais do jogo.
        """
        if num_to_select != 15 and self.target_sum_range: # Se avalia propriedades, idealmente são 15
            print(f"AVISO ({self.get_name()}): Esta estratégia é otimizada para selecionar 15 dezenas "
                  "devido à avaliação de propriedades. Selecionando {num_to_select} dezenas.")
            # Se num_to_select != 15, a avaliação de propriedades pode não fazer sentido ou precisar de adaptação.
            # Por simplicidade, se não for 15, pode recorrer à seleção padrão.
            # return super().select_numbers(scores_df, num_to_select, selection_params)


        candidate_dezenas = scores_df.head(self.candidate_pool_size)['dezena'].tolist()

        if len(candidate_dezenas) < num_to_select:
            print(f"AVISO ({self.get_name()}): Pool de candidatos ({len(candidate_dezenas)}) "
                  f"menor que o número a selecionar ({num_to_select}). Retornando top N scores individuais.")
            return sorted(scores_df.head(num_to_select)['dezena'].tolist())

        best_combination: Optional[List[int]] = None
        max_property_score = -1.0 # Inicializa com -1 para garantir que qualquer score positivo seja melhor

        # Calcula o número de combinações possíveis para decidir se a avaliação completa é viável
        num_possible_combos = 0
        if len(candidate_dezenas) >= num_to_select:
            try:
                num_possible_combos = math.comb(len(candidate_dezenas), num_to_select)
            except AttributeError: # math.comb é Python 3.8+
                from scipy.special import comb # Fallback para scipy
                num_possible_combos = comb(len(candidate_dezenas), num_to_select, exact=True)

        
        print(f"INFO ({self.get_name()}): Pool de {len(candidate_dezenas)} dezenas. Gerando combinações de {num_to_select}. "
              f"Combinações possíveis: {num_possible_combos}. Limite de avaliação: {self.max_combinations_to_evaluate}")
        
        # Se o número de combinações for gerenciável, gera todas.
        # Caso contrário, precisaria de amostragem ou heurística.
        iterator_combinations = combinations(candidate_dezenas, num_to_select)
        
        # Se for exceder o limite, e quisermos uma amostra aleatória em vez de apenas as primeiras N:
        # import random
        # if num_possible_combos > self.max_combinations_to_evaluate:
        #     all_combinations = list(iterator_combinations) # Materializa todas, pode ser problemático
        #     iterator_combinations = random.sample(all_combinations, self.max_combinations_to_evaluate)
        #     print(f"INFO ({self.get_name()}): Amostrando {self.max_combinations_to_evaluate} combinações aleatoriamente.")

        count = 0
        for combo_tuple in iterator_combinations:
            if count >= self.max_combinations_to_evaluate and num_possible_combos > self.max_combinations_to_evaluate :
                print(f"AVISO ({self.get_name()}): Limite de {self.max_combinations_to_evaluate} combinações atingido para avaliação.")
                break
            
            combo_list = list(combo_tuple)
            property_score = self._score_combination_properties(combo_list)

            if property_score > max_property_score:
                max_property_score = property_score
                best_combination = combo_list
            count += 1
            if count % 10000 == 0 and count > 0: # Log de progresso para muitas combinações
                print(f"INFO ({self.get_name()}): Avaliadas {count} combinações...")


        if best_combination:
            print(f"INFO ({self.get_name()}): Melhor combinação encontrada com score de propriedade: {max_property_score:.4f}")
            return sorted(best_combination)
        else:
            # Se nenhuma combinação foi avaliada ou nenhuma atingiu um score > -1 (o que é improvável se houver combinações)
            print(f"AVISO ({self.get_name()}): Nenhuma combinação adequada encontrada (ou limite de avaliação/pool baixo). "
                  "Retornando top N dezenas com base no score individual de itemsets.")
            return sorted(scores_df.head(num_to_select)['dezena'].tolist())