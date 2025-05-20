# src/strategies/trend_recurrence_strategy.py
from typing import List, Optional, Dict, Any
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Supondo que BaseStrategy e os componentes do Aggregator/DBManager são importáveis
# Ajuste os caminhos de importação conforme a estrutura do seu projeto.
from .base_strategy import BaseStrategy
from ..database_manager import DatabaseManager
from ..analysis_aggregator import AnalysisAggregator
# from ..config import config as app_config

class TrendAndRecurrenceStrategy(BaseStrategy):
    """
    Estratégia que foca em dezenas com tendência de ranking ascendente
    e alta probabilidade de recorrência baseada no atraso atual,
    utilizando o AnalysisAggregator para obter os dados.
    """

    def __init__(self,
                 db_manager: DatabaseManager,
                 config: Dict[str, Any], # Config global do app
                 analysis_aggregator: AnalysisAggregator,
                 # Parâmetros específicos da estratégia:
                 trend_weight: float = 0.6,
                 recurrence_weight: float = 0.4,
                 min_recurrence_cdf_filter: float = 0.0 # Filtro opcional para CDF mínimo antes da pontuação
                 ):
        super().__init__(db_manager, config, analysis_aggregator,
                         trend_weight=trend_weight,
                         recurrence_weight=recurrence_weight,
                         min_recurrence_cdf_filter=min_recurrence_cdf_filter)
        
        self.trend_weight = trend_weight
        self.recurrence_weight = recurrence_weight
        self.min_recurrence_cdf_filter = min_recurrence_cdf_filter
        
        # Cache para dados do agregador
        self._data_cache: Dict[str, pd.DataFrame] = {}

        if not (0 <= self.trend_weight <= 1 and 0 <= self.recurrence_weight <= 1):
            raise ValueError("Os pesos de tendência e recorrência devem estar entre 0 e 1.")
        if not (self.trend_weight + self.recurrence_weight > 0): # Pelo menos um peso deve ser positivo
            # Considerar se a soma dos pesos deve ser 1.0, ou se são apenas relativos.
            # Se a soma precisar ser 1, adicione validação ou normalize os pesos.
            print(f"AVISO ({self.get_name()}): A soma dos pesos é 0. Scores podem não ser significativos.")


    def get_name(self) -> str:
        return (f"TrendAndRecurrenceStrategy(trend_w={self.trend_weight}, "
                f"recur_w={self.recurrence_weight}, min_cdf_filter={self.min_recurrence_cdf_filter})")

    def get_description(self) -> str:
        return ("Combina scores normalizados de tendência de ranking (coluna 'rank_slope' do Aggregator) e "
                "probabilidade de recorrência (coluna 'recurrence_cdf' do Aggregator). "
                f"Aplica um filtro opcional de CDF de recorrência mínimo de {self.min_recurrence_cdf_filter} antes de pontuar.")

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

    def _get_rank_trend_metrics_df(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Extrai métricas de tendência de rank do DataFrame consolidado do AnalysisAggregator.
        Espera uma coluna como 'rank_slope'.
        Retorna DataFrame com colunas ['dezena', 'trend_metric'].
        """
        df_aggregated_metrics = self._fetch_and_cache_aggregated_data(latest_draw_id)
        
        target_col_trend = self.config.get('aggregator_col_rank_trend_slope', 'rank_slope') # Nome da coluna do config ou default

        if df_aggregated_metrics.empty or target_col_trend not in df_aggregated_metrics.columns:
            print(f"AVISO ({self.get_name()}): Coluna de tendência '{target_col_trend}' não encontrada nos dados agregados. "
                  "Verifique se o AnalysisAggregator a fornece. Usando métrica de tendência 0 para todas as dezenas.")
            return pd.DataFrame({'dezena': self._all_dezenas_list, 'trend_metric': 0.0})
            
        return df_aggregated_metrics[['dezena', target_col_trend]].rename(
            columns={target_col_trend: 'trend_metric'}
        )

    def _get_recurrence_cdf_df(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Extrai a probabilidade de recorrência (CDF do atraso atual) do DataFrame do AnalysisAggregator.
        Espera uma coluna como 'recurrence_cdf'.
        Retorna DataFrame com colunas ['dezena', 'recurrence_cdf'].
        """
        df_aggregated_metrics = self._fetch_and_cache_aggregated_data(latest_draw_id)
        
        target_col_cdf = self.config.get('aggregator_col_recurrence_cdf', 'recurrence_cdf')

        if df_aggregated_metrics.empty or target_col_cdf not in df_aggregated_metrics.columns:
            print(f"AVISO ({self.get_name()}): Coluna de CDF de recorrência '{target_col_cdf}' não encontrada. "
                  "Usando CDF 0 para todas as dezenas.")
            return pd.DataFrame({'dezena': self._all_dezenas_list, 'recurrence_cdf': 0.0})
            
        return df_aggregated_metrics[['dezena', target_col_cdf]].rename(
            columns={target_col_cdf: 'recurrence_cdf'}
        )

    def generate_scores(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        df_trend_metrics = self._get_rank_trend_metrics_df(latest_draw_id)
        df_recurrence_cdf = self._get_recurrence_cdf_df(latest_draw_id)

        base_dezenas_df = pd.DataFrame({'dezena': self._all_dezenas_list})
        df_merged = pd.merge(base_dezenas_df, df_trend_metrics, on='dezena', how='left')
        df_merged = pd.merge(df_merged, df_recurrence_cdf, on='dezena', how='left')
        
        # Preenchimento de NaNs
        df_merged['trend_metric'] = df_merged['trend_metric'].fillna(0.0)
        df_merged['recurrence_cdf'] = df_merged['recurrence_cdf'].fillna(0.0)

        if df_merged.empty: # Pouco provável se base_dezenas_df for usado
            return pd.DataFrame(columns=['dezena', 'score', 'ranking_strategy'])
            
        # Aplicar filtro opcional de CDF mínimo ANTES da pontuação e normalização
        if self.min_recurrence_cdf_filter > 0:
            df_merged = df_merged[df_merged['recurrence_cdf'] >= self.min_recurrence_cdf_filter].copy()
            if df_merged.empty:
                 print(f"AVISO ({self.get_name()}): Nenhuma dezena passou pelo filtro min_recurrence_cdf_filter={self.min_recurrence_cdf_filter}.")
                 return pd.DataFrame({'dezena': [], 'score': [], 'ranking_strategy': []}) # Retorna DF vazio com colunas


        # Se após o filtro o df_merged ficar vazio, não há o que pontuar.
        if df_merged.empty:
            return pd.DataFrame({'dezena': [], 'score': [], 'ranking_strategy': []})

        scaler = MinMaxScaler()

        # Normalizar métrica de tendência
        if 'trend_metric' in df_merged.columns and df_merged['trend_metric'].nunique() > 1:
            df_merged['trend_score_norm'] = scaler.fit_transform(df_merged[['trend_metric']])
        elif 'trend_metric' in df_merged.columns:
            df_merged['trend_score_norm'] = 0.5 
        else:
            df_merged['trend_score_norm'] = 0.0
        
        # Normalizar CDF de recorrência (embora já seja 0-1, escalar pode ajudar se a distribuição for ruim)
        if 'recurrence_cdf' in df_merged.columns and df_merged['recurrence_cdf'].nunique() > 1:
            df_merged['recurrence_score_norm'] = scaler.fit_transform(df_merged[['recurrence_cdf']])
        elif 'recurrence_cdf' in df_merged.columns:
            df_merged['recurrence_score_norm'] = df_merged['recurrence_cdf'].iloc[0] if not df_merged.empty else 0.5 # Usa o valor único ou 0.5
        else:
            df_merged['recurrence_score_norm'] = 0.0

        # Score final combinado
        df_merged['score'] = (self.trend_weight * df_merged['trend_score_norm'] +
                              self.recurrence_weight * df_merged['recurrence_score_norm'])

        df_final_scores = df_merged.sort_values(by='score', ascending=False).reset_index(drop=True)
        df_final_scores['ranking_strategy'] = df_final_scores.index + 1
        
        return df_final_scores[['dezena', 'score', 'ranking_strategy']]