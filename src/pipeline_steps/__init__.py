# src/pipeline_steps/__init__.py
import logging
logger = logging.getLogger(__name__)

# Importar funções dos módulos de etapa existentes
# Se um destes imports falhar, significa que há um erro DENTRO do arquivo .py correspondente
# ou a função nomeada não está definida nele.

from .execute_frequency import run_frequency_analysis
from .execute_delay import run_delay_analysis
from .execute_properties import run_number_properties_analysis
from .execute_pairs import run_pair_combination_analysis # Assumindo que execute_pairs.py tem esta função
                                                       # e é a que "pair-combination-analysis" deve chamar.
                                                       # Se for de execute_combinations.py, ajuste o import.

# Para as análises de ciclo, precisamos dos nomes corretos das funções e arquivos.
# Se execute_cycle_stats.py tem run_cycle_identification_and_stats:
from .execute_cycle_stats import run_cycle_identification_and_stats
# Se execute_cycles.py tem run_cycle_closing_analysis:
from .execute_cycles import run_cycle_closing_analysis


# Etapas que você ainda precisa implementar os arquivos .py em src/pipeline_steps/
# e as funções de análise correspondentes em src/analysis/
# Por enquanto, vamos definir stubs para que o main.py não quebre ao tentar referenciá-los via 'ps.'
# se eles estiverem listados no ANALYSIS_PIPELINE.
def run_repetition_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_repetition_analysis' chamada, mas não totalmente implementada em src/pipeline_steps/execute_repetition.py ou src/analysis/repetition_analysis.py")
    return False

def run_group_trend_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_group_trend_analysis' chamada, mas não totalmente implementada.")
    return False

def run_rank_trend_analysis(*args, **kwargs):
    logger.error("ETAPA STUB: 'run_rank_trend_analysis' chamada, mas não totalmente implementada.")
    return False


# Visualizações
from .execute_metrics_viz import run_core_metrics_visualization

# Novas etapas de Chunk (estas devem estar funcionando)
from .execute_chunk_evolution_analysis import run_chunk_evolution_analysis
from .execute_chunk_evolution_visualization import run_chunk_evolution_visualization

# Opcional: execute_max_delay.py
# Se você quiser usar run_max_delay_analysis_step como uma etapa separada no pipeline,
# importe-a aqui. Caso contrário, se for parte de run_delay_analysis, não precisa.
# from .execute_max_delay import run_max_delay_analysis_step


__all__ = [
    "run_frequency_analysis",
    "run_delay_analysis",
    "run_number_properties_analysis",
    "run_repetition_analysis",        # Aponta para o stub se não implementado
    "run_pair_combination_analysis",
    "run_cycle_identification_and_stats",
    "run_cycle_closing_analysis",
    "run_group_trend_analysis",       # Aponta para o stub se não implementado
    "run_rank_trend_analysis",        # Aponta para o stub se não implementado
    "run_core_metrics_visualization",
    "run_chunk_evolution_analysis",
    "run_chunk_evolution_visualization",
    # "run_max_delay_analysis_step", # Adicione se for usar
]