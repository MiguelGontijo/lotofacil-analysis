# src/strategies/simple_recency_delay_strategy.py
from typing import List, Optional, Dict, Any
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Supondo que BaseStrategy e os componentes do Aggregator/DBManager são importáveis
# Ajuste os caminhos de importação conforme a estrutura do seu projeto.
# Se os arquivos estiverem em src/
from .base_strategy import BaseStrategy
from ..database_manager import DatabaseManager
from ..analysis_aggregator import AnalysisAggregator
# from ..config import config as app_config # Se você importar o config global

class SimpleRecencyAndDelayStrategy(BaseStrategy):
    """
    Estratégia que prioriza dezenas com base na frequência recente (recência)
    e no atraso atual, utilizando o AnalysisAggregator para obter os dados.
    """

    def __init__(self,
                 db_manager: DatabaseManager,
                 config: Dict[str, Any], # Config global do app
                 analysis_aggregator: AnalysisAggregator,
                 # Parâmetros específicos da estratégia:
                 target_recent_window_suffix: str = "10", # Ex: "10" para usar 'recent_frequency_window_10' do Aggregator
                 delay_weight: float = 0.5,
                 frequency_weight: float = 0.5
                 ):
        super().__init__(db_manager, config, analysis_aggregator,
                         # Passando os parâmetros específicos para BaseStrategy se ela os armazenar
                         target_recent_window_suffix=target_recent_window_suffix,
                         delay_weight=delay_weight,
                         frequency_weight=frequency_weight)
        
        # O nome da coluna de frequência recente esperada do Aggregator
        self.target_recent_window_col = f"recent_frequency_window_{target_recent_window_suffix}"
        
        self.delay_weight = delay_weight
        self.frequency_weight = frequency_weight
        
        # Cache para dados do agregador, para evitar múltiplas chamadas com o mesmo latest_draw_id
        self._data_cache: Dict[str, pd.DataFrame] = {}

        if not (0 <= self.delay_weight <= 1 and 0 <= self.frequency_weight <= 1 and (self.delay_weight + self.frequency_weight > 0)):
            # Permitir soma > 1 se os pesos forem relativos, mas não negativos e pelo menos um > 0.
            # Se a soma dos pesos positivos deve ser 1, uma validação adicional seria necessária.
            raise ValueError("Os pesos de delay e frequência devem estar entre 0 e 1, e pelo menos um deve ser positivo.")

    def get_name(self) -> str:
        # O nome da estratégia pode incluir seus parâmetros principais para fácil identificação
        return (f"SimpleRecencyAndDelayStrategy(target_window_col={self.target_recent_window_col}, "
                f"delay_w={self.delay_weight}, freq_w={self.frequency_weight})")

    def get_description(self) -> str:
        return (f"Combina scores normalizados de frequência recente (usando a coluna {self.target_recent_window_col} "
                "do AnalysisAggregator) e atraso atual das dezenas.")

    def _fetch_and_cache_aggregated_data(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Busca dados do AnalysisAggregator e os armazena em cache na instância da estratégia
        para o latest_draw_id fornecido.
        """
        cache_key = str(latest_draw_id) if latest_draw_id is not None else "latest_overall"
        
        if cache_key not in self._data_cache:
            # print(f"INFO (Strategy:{self.get_name()}): Cache miss. Buscando dados agregados para concurso/ponto {cache_key}")
            self._data_cache[cache_key] = self.analysis_aggregator.get_historical_metrics_for_dezenas(
                latest_concurso_id=latest_draw_id
            )
        # else:
            # print(f"INFO (Strategy:{self.get_name()}): Cache hit para concurso/ponto {cache_key}")
        return self._data_cache[cache_key].copy() # Retorna cópia para evitar modificação acidental do cache

    def _get_recent_frequency_df(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Extrai a frequência recente do DataFrame consolidado fornecido pelo AnalysisAggregator.
        Retorna um DataFrame com colunas ['dezena', 'recent_frequency'].
        """
        df_aggregated_metrics = self._fetch_and_cache_aggregated_data(latest_draw_id)

        if df_aggregated_metrics.empty or self.target_recent_window_col not in df_aggregated_metrics.columns:
            print(f"AVISO ({self.get_name()}): Coluna de frequência recente '{self.target_recent_window_col}' "
                  "não encontrada nos dados agregados. Verifique se o AnalysisAggregator a fornece ou ajuste "
                  f"o parâmetro 'target_recent_window_suffix'. Usando frequência 0 para todas as dezenas.")
            return pd.DataFrame({'dezena': self._all_dezenas_list, 'recent_frequency': 0.0})
        
        return df_aggregated_metrics[['dezena', self.target_recent_window_col]].rename(
            columns={self.target_recent_window_col: 'recent_frequency'}
        )

    def _get_current_delays_df(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Extrai os atrasos atuais do DataFrame consolidado fornecido pelo AnalysisAggregator.
        Retorna um DataFrame com colunas ['dezena', 'current_delay'].
        """
        df_aggregated_metrics = self._fetch_and_cache_aggregated_data(latest_draw_id)

        if df_aggregated_metrics.empty or 'current_delay' not in df_aggregated_metrics.columns:
            print(f"AVISO ({self.get_name()}): Coluna 'current_delay' não encontrada nos dados agregados. "
                  "Usando atraso 0 para todas as dezenas.")
            return pd.DataFrame({'dezena': self._all_dezenas_list, 'current_delay': 0.0})
            
        return df_aggregated_metrics[['dezena', 'current_delay']]

    def generate_scores(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        # Os métodos _get_ já utilizam o _fetch_and_cache_aggregated_data internamente
        df_recent_freq = self._get_recent_frequency_df(latest_draw_id)
        df_current_delays = self._get_current_delays_df(latest_draw_id)

        # Merge dos dataframes de métricas (embora venham da mesma fonte agregada,
        # esta etapa garante o alinhamento e a presença de todas as dezenas).
        # O _all_dezenas_list é herdado de BaseStrategy ou pode ser pego do aggregator.
        base_dezenas_df = pd.DataFrame({'dezena': self._all_dezenas_list})
        
        df_merged = pd.merge(base_dezenas_df, df_recent_freq, on='dezena', how='left')
        df_merged = pd.merge(df_merged, df_current_delays, on='dezena', how='left')
        
        # Preencher NaNs que podem surgir se alguma dezena não estiver no resultado do aggregator
        # (embora o aggregator deva idealmente retornar todas as dezenas com fillna apropriado).
        # Para 'recent_frequency' e 'current_delay', 0 é um default razoável se a informação estiver ausente.
        df_merged['recent_frequency'] = df_merged['recent_frequency'].fillna(0)
        df_merged['current_delay'] = df_merged['current_delay'].fillna(0)
        
        if df_merged.empty: # Pouco provável se base_dezenas_df for usado
            return pd.DataFrame(columns=['dezena', 'score', 'ranking_strategy'])

        # Normalização dos scores (MinMax para colocar entre 0 e 1)
        scaler = MinMaxScaler()
        
        # Normalizar frequência recente
        if 'recent_frequency' in df_merged.columns and df_merged['recent_frequency'].nunique() > 1:
            df_merged['freq_score_norm'] = scaler.fit_transform(df_merged[['recent_frequency']])
        elif 'recent_frequency' in df_merged.columns: # Existe, mas todos os valores são iguais
            df_merged['freq_score_norm'] = 0.5 # Atribui um valor neutro
        else: # Coluna não existe
            df_merged['freq_score_norm'] = 0.0

        # Normalizar atraso atual
        if 'current_delay' in df_merged.columns and df_merged['current_delay'].nunique() > 1:
            df_merged['delay_score_norm'] = scaler.fit_transform(df_merged[['current_delay']])
        elif 'current_delay' in df_merged.columns: # Existe, mas todos os valores são iguais
            df_merged['delay_score_norm'] = 0.5 # Atribui um valor neutro
        else: # Coluna não existe
            df_merged['delay_score_norm'] = 0.0
            
        # Score final combinado pelos pesos
        df_merged['score'] = (self.frequency_weight * df_merged['freq_score_norm'] +
                              self.delay_weight * df_merged['delay_score_norm'])

        # Ordenar e adicionar ranking
        df_final_scores = df_merged.sort_values(by='score', ascending=False).reset_index(drop=True)
        df_final_scores['ranking_strategy'] = df_final_scores.index + 1

        return df_final_scores[['dezena', 'score', 'ranking_strategy']]