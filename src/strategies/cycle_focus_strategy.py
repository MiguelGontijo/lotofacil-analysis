# src/strategies/cycle_focus_strategy.py
from typing import List, Optional, Dict, Any
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Supondo que BaseStrategy e os componentes do Aggregator/DBManager são importáveis
# Ajuste os caminhos de importação conforme a estrutura do seu projeto.
from .base_strategy import BaseStrategy
from ..database_manager import DatabaseManager
from ..analysis_aggregator import AnalysisAggregator
# from ..config import config as app_config

class CycleFocusStrategy(BaseStrategy):
    """
    Estratégia que pontua dezenas com base no seu status e comportamento
    dentro dos ciclos da Lotofácil, utilizando o AnalysisAggregator.
    Prioriza dezenas para fechar ciclos, aquelas com atraso relevante no contexto
    do ciclo e com comportamento histórico favorável em fechamentos.
    """

    def __init__(self,
                 db_manager: DatabaseManager,
                 config: Dict[str, Any], # Config global do app
                 analysis_aggregator: AnalysisAggregator,
                 # Parâmetros específicos da estratégia:
                 missing_in_cycle_weight: float = 0.5, # Aumentei o peso default
                 sub_cycle_delay_weight: float = 0.3,
                 closing_behavior_weight: float = 0.2  # Ajustei para somar 1 com os outros defaults
                 ):
        super().__init__(db_manager, config, analysis_aggregator,
                         missing_in_cycle_weight=missing_in_cycle_weight,
                         sub_cycle_delay_weight=sub_cycle_delay_weight,
                         closing_behavior_weight=closing_behavior_weight)
        
        self.missing_in_cycle_weight = self.strategy_specific_params.get('missing_in_cycle_weight')
        self.sub_cycle_delay_weight = self.strategy_specific_params.get('sub_cycle_delay_weight')
        self.closing_behavior_weight = self.strategy_specific_params.get('closing_behavior_weight')
        
        # Cache para dados do agregador
        self._data_cache: Dict[str, pd.DataFrame] = {}
        
        # Validação dos pesos
        total_weight = self.missing_in_cycle_weight + self.sub_cycle_delay_weight + self.closing_behavior_weight
        if not (total_weight > 0): # Pelo menos um peso deve ser efetivo
            print(f"AVISO ({self.get_name()}): A soma dos pesos é {total_weight}. "
                  "Scores podem não ser significativos se todos os pesos forem zero.")
        # Se você quiser que a soma seja exatamente 1, adicione uma normalização ou validação aqui.

    def get_name(self) -> str:
        return (f"CycleFocusStrategy(missing_w={self.missing_in_cycle_weight:.2f}, "
                f"sub_delay_w={self.sub_cycle_delay_weight:.2f}, closing_w={self.closing_behavior_weight:.2f})")

    def get_description(self) -> str:
        return ("Pontua dezenas com base em: importância para fechar ciclos (coluna 'is_missing_in_current_cycle'), "
                "atraso atual ('current_delay' usado como proxy para atraso sub-ciclo), e "
                "comportamento histórico em fechamentos de ciclo (coluna 'cycle_closing_propensity_score'). "
                "Todos os dados são via AnalysisAggregator.")

    def _fetch_and_cache_aggregated_data(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Busca dados do AnalysisAggregator e os armazena em cache na instância da estratégia.
        """
        cache_key = str(latest_draw_id) if latest_draw_id is not None else "latest_overall"
        
        if cache_key not in self._data_cache:
            # print(f"INFO (Strategy:{self.get_name()}): Cache miss. Buscando dados agregados para concurso/ponto {cache_key}")
            self._data_cache[cache_key] = self.analysis_aggregator.get_historical_metrics_for_dezenas(
                latest_concurso_id=latest_draw_id
            )
        # else:
            # print(f"INFO (Strategy:{self.get_name()}): Cache hit para concurso/ponto {cache_key}")
        return self._data_cache[cache_key].copy()

    def _get_missing_dezenas_scores_df(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Extrai o status de 'is_missing_in_current_cycle' do DataFrame do Aggregator.
        Retorna DataFrame com ['dezena', 'missing_score'].
        """
        df_aggregated_metrics = self._fetch_and_cache_aggregated_data(latest_draw_id)
        
        # Nome da coluna esperado do Aggregator (pode vir do config para flexibilidade)
        target_col_missing = self.config.get('aggregator_col_is_missing_in_cycle', 'is_missing_in_current_cycle')

        if df_aggregated_metrics.empty or target_col_missing not in df_aggregated_metrics.columns:
            print(f"AVISO ({self.get_name()}): Coluna '{target_col_missing}' não encontrada nos dados agregados. "
                  "Assumindo score 0 para 'missing_score' para todas as dezenas.")
            return pd.DataFrame({'dezena': self._all_dezenas_list, 'missing_score': 0.0})
            
        df_missing = df_aggregated_metrics[['dezena', target_col_missing]].copy()
        # A coluna 'is_missing_in_current_cycle' deve ser 0 ou 1. Convertendo para float para consistência.
        df_missing['missing_score'] = df_missing[target_col_missing].astype(float)
        return df_missing[['dezena', 'missing_score']]

    def _get_sub_cycle_delay_scores_df(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Usa 'current_delay' do Aggregator como base para o score de atraso sub-ciclo.
        Normaliza o 'current_delay'.
        Retorna DataFrame com ['dezena', 'sub_cycle_delay_score'].
        """
        df_aggregated_metrics = self._fetch_and_cache_aggregated_data(latest_draw_id)
        
        target_col_delay = self.config.get('aggregator_col_current_delay', 'current_delay')

        if df_aggregated_metrics.empty or target_col_delay not in df_aggregated_metrics.columns:
            print(f"AVISO ({self.get_name()}): Coluna '{target_col_delay}' não encontrada. "
                  "Usando 0 para 'sub_cycle_delay_score' para todas as dezenas.")
            return pd.DataFrame({'dezena': self._all_dezenas_list, 'sub_cycle_delay_score': 0.0})

        df_sub_delay = df_aggregated_metrics[['dezena', target_col_delay]].copy()
        df_sub_delay = df_sub_delay.rename(columns={target_col_delay: 'raw_sub_cycle_delay'})

        scaler = MinMaxScaler()
        if 'raw_sub_cycle_delay' in df_sub_delay.columns and df_sub_delay['raw_sub_cycle_delay'].nunique() > 1:
            df_sub_delay['sub_cycle_delay_score'] = scaler.fit_transform(df_sub_delay[['raw_sub_cycle_delay']])
        elif 'raw_sub_cycle_delay' in df_sub_delay.columns: # Coluna existe, mas todos os valores são iguais
            df_sub_delay['sub_cycle_delay_score'] = 0.5 # Valor neutro
        else: # Coluna não existe (improvável, mas defensivo)
             df_sub_delay['sub_cycle_delay_score'] = 0.0
        
        return df_sub_delay[['dezena', 'sub_cycle_delay_score']]

    def _get_cycle_closing_behavior_scores_df(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Extrai 'cycle_closing_propensity_score' (ou similar) do Aggregator.
        Este score reflete a tendência histórica da dezena aparecer em fechamentos de ciclo.
        Retorna DataFrame com ['dezena', 'closing_behavior_score'].
        """
        df_aggregated_metrics = self._fetch_and_cache_aggregated_data(latest_draw_id)
        
        target_col_closing = self.config.get('aggregator_col_cycle_closing_propensity', 'cycle_closing_propensity_score')

        if df_aggregated_metrics.empty or target_col_closing not in df_aggregated_metrics.columns:
            print(f"AVISO ({self.get_name()}): Coluna '{target_col_closing}' não encontrada. "
                  "Usando 0 para 'closing_behavior_score' para todas as dezenas.")
            return pd.DataFrame({'dezena': self._all_dezenas_list, 'closing_behavior_score': 0.0})

        df_closing = df_aggregated_metrics[['dezena', target_col_closing]].copy()
        
        # Assumindo que o score do aggregator já está numa escala razoável ou é normalizado aqui.
        # Se o score do agregador for, por exemplo, uma contagem, a normalização é crucial.
        scaler = MinMaxScaler()
        if target_col_closing in df_closing.columns and df_closing[target_col_closing].nunique() > 1:
            df_closing['closing_behavior_score'] = scaler.fit_transform(df_closing[[target_col_closing]])
        elif target_col_closing in df_closing.columns: # Coluna existe, mas valores iguais
            # Se for um score já normalizado (ex: todos 0.5), usa esse valor.
            # Se for uma contagem (ex: todos 0), o scaler pode não funcionar bem, então 0.5 é um default.
            # O ideal é que o agregador forneça scores já significativos ou normalizáveis.
            df_closing['closing_behavior_score'] = 0.5 if df_closing[target_col_closing].iloc[0] != 0 else 0.0
        else: # Coluna não existe
             df_closing['closing_behavior_score'] = 0.0
            
        return df_closing[['dezena', 'closing_behavior_score']]

    def generate_scores(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        df_missing_scores = self._get_missing_dezenas_scores_df(latest_draw_id)
        df_sub_delay_scores = self._get_sub_cycle_delay_scores_df(latest_draw_id)
        df_closing_behavior_scores = self._get_cycle_closing_behavior_scores_df(latest_draw_id)

        # Merge dos dataframes de scores parciais
        # _all_dezenas_list é herdado da BaseStrategy
        base_dezenas_df = pd.DataFrame({'dezena': self._all_dezenas_list})
        df_merged = pd.merge(base_dezenas_df, df_missing_scores, on='dezena', how='left')
        df_merged = pd.merge(df_merged, df_sub_delay_scores, on='dezena', how='left')
        df_merged = pd.merge(df_merged, df_closing_behavior_scores, on='dezena', how='left')
        
        # Preencher NaNs que podem surgir dos merges (embora os métodos _get_ devam retornar todas as dezenas)
        df_merged = df_merged.fillna({
            'missing_score': 0.0, 
            'sub_cycle_delay_score': 0.0, 
            'closing_behavior_score': 0.0
        })

        if df_merged.empty: # Improvável se base_dezenas_df for usado
            return pd.DataFrame(columns=['dezena', 'score', 'ranking_strategy'])

        # Score final combinado pelos pesos
        df_merged['score'] = (
            self.missing_in_cycle_weight * df_merged['missing_score'] +
            self.sub_cycle_delay_weight * df_merged['sub_cycle_delay_score'] +
            self.closing_behavior_weight * df_merged['closing_behavior_score']
        )

        # Ordenar e adicionar ranking
        df_final_scores = df_merged.sort_values(by='score', ascending=False).reset_index(drop=True)
        df_final_scores['ranking_strategy'] = df_final_scores.index + 1

        return df_final_scores[['dezena', 'score', 'ranking_strategy']]

    # O método select_numbers usará a implementação padrão da BaseStrategy (top N scores),
    # a menos que uma lógica de seleção mais específica para ciclos seja desejada.