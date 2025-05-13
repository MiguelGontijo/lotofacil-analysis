# src/pipeline_steps/__init__.py
import logging

logger = logging.getLogger(__name__)

# Tentativa de importação de todas as funções de etapa conhecidas/fornecidas.
# Se um arquivo não existir, o import falhará, o que é esperado se o módulo não estiver lá.

try:
    from .execute_frequency import run_frequency_analysis
except ImportError:
    logger.warning("Falha ao importar 'run_frequency_analysis' de .execute_frequency")
    def run_frequency_analysis(*args, **kwargs): logger.error("Etapa 'run_frequency_analysis' não encontrada."); return False

try:
    from .execute_delay import run_delay_analysis
except ImportError:
    logger.warning("Falha ao importar 'run_delay_analysis' de .execute_delay")
    def run_delay_analysis(*args, **kwargs): logger.error("Etapa 'run_delay_analysis' não encontrada."); return False

try:
    from .execute_properties import run_number_properties_analysis
except ImportError:
    logger.warning("Falha ao importar 'run_number_properties_analysis' de .execute_properties")
    def run_number_properties_analysis(*args, **kwargs): logger.error("Etapa 'run_number_properties_analysis' não encontrada."); return False

# Para repetição, group_trend, rank_trend, cycle_closing:
# Você precisará criar os arquivos .py correspondentes (ex: execute_repetition.py)
# e definir as funções (ex: run_repetition_analysis) neles.
# Por enquanto, vou definir stubs para evitar ImportError se eles não existirem,
# mas o ideal é que os arquivos e funções sejam criados.

# Supondo que você tenha um src/pipeline_steps/execute_repetition.py
try:
    from .execute_repetition import run_repetition_analysis # Você precisará criar este arquivo e função
except ImportError:
    logger.warning("Falha ao importar 'run_repetition_analysis' de .execute_repetition. Crie o arquivo/função.")
    def run_repetition_analysis(*args, **kwargs): logger.error("Etapa 'run_repetition_analysis' não implementada/encontrada."); return False

# Para execute_pairs.py que contém run_pair_combination_analysis
try:
    from .execute_pairs import run_pair_combination_analysis
except ImportError:
    # Se execute_pairs.py não existir, talvez a função esteja em execute_combinations.py
    try:
        from .execute_combinations import run_pair_combination_analysis
    except ImportError:
        logger.warning("Falha ao importar 'run_pair_combination_analysis' de .execute_pairs ou .execute_combinations")
        def run_pair_combination_analysis(*args, **kwargs): logger.error("Etapa 'run_pair_combination_analysis' não encontrada."); return False

# Para execute_cycle_stats.py
try:
    from .execute_cycle_stats import run_cycle_identification_and_stats
except ImportError:
    logger.warning("Falha ao importar 'run_cycle_identification_and_stats' de .execute_cycle_stats")
    def run_cycle_identification_and_stats(*args, **kwargs): logger.error("Etapa 'run_cycle_identification_and_stats' não encontrada."); return False

# Para execute_cycles.py (que eu assumi ser para run_cycle_closing_analysis)
try:
    from .execute_cycles import run_cycle_closing_analysis # Se a função se chama assim em execute_cycles.py
except ImportError:
    logger.warning("Falha ao importar 'run_cycle_closing_analysis' de .execute_cycles")
    def run_cycle_closing_analysis(*args, **kwargs): logger.error("Etapa 'run_cycle_closing_analysis' não encontrada."); return False


# Para execute_group_trend.py
try:
    from .execute_group_trend import run_group_trend_analysis # Você precisará criar este arquivo e função
except ImportError:
    logger.warning("Falha ao importar 'run_group_trend_analysis' de .execute_group_trend. Crie o arquivo/função.")
    def run_group_trend_analysis(*args, **kwargs): logger.error("Etapa 'run_group_trend_analysis' não implementada/encontrada."); return False

# Para execute_rank_trend.py
try:
    from .execute_rank_trend import run_rank_trend_analysis # Você precisará criar este arquivo e função
except ImportError:
    logger.warning("Falha ao importar 'run_rank_trend_analysis' de .execute_rank_trend. Crie o arquivo/função.")
    def run_rank_trend_analysis(*args, **kwargs): logger.error("Etapa 'run_rank_trend_analysis' não implementada/encontrada."); return False

# Para execute_metrics_viz.py
try:
    from .execute_metrics_viz import run_core_metrics_visualization
except ImportError:
    logger.warning("Falha ao importar 'run_core_metrics_visualization' de .execute_metrics_viz")
    def run_core_metrics_visualization(*args, **kwargs): logger.error("Etapa 'run_core_metrics_visualization' não encontrada."); return False

# NOVAS importações para análise e visualização de chunks (estes arquivos foram fornecidos por mim)
from .execute_chunk_evolution_analysis import run_chunk_evolution_analysis
from .execute_chunk_evolution_visualization import run_chunk_evolution_visualization

# Opcional: execute_max_delay.py (se você decidir usá-lo como uma etapa separada)
try:
    from .execute_max_delay import run_max_delay_analysis_step
except ImportError:
    logger.info("Etapa opcional 'run_max_delay_analysis_step' não encontrada em .execute_max_delay (normal se não for usada).")
    # Não é crítico se não for usado no pipeline do main.py

__all__ = [
    "run_frequency_analysis",
    "run_delay_analysis",
    "run_number_properties_analysis",
    "run_repetition_analysis",
    "run_pair_combination_analysis", # Assumindo que esta é a função correta para combinações de pares
    "run_cycle_identification_and_stats",
    "run_cycle_closing_analysis", # Assumindo que esta é a função correta para fechamento de ciclo
    "run_group_trend_analysis",
    "run_rank_trend_analysis",
    "run_core_metrics_visualization",
    "run_chunk_evolution_analysis",
    "run_chunk_evolution_visualization",
    # "run_max_delay_analysis_step", # Adicione se for usar como etapa no main.py
]

# Verifique se os nomes das funções em __all__ correspondem exatamente aos nomes
# das funções importadas e aos nomes usados no ANALYSIS_PIPELINE em main.py (após ps.).