# src/pipeline_steps/execute_chunk_evolution_visualization.py
import logging
from pathlib import Path # Importação de Path já estava correta
from typing import Optional # <<< --- ADICIONADO IMPORT DE OPTIONAL

from src.visualization.plotter import plot_chunk_metric_evolution
from src.database_manager import DatabaseManager
from src.config import (
    DEFAULT_CHUNK_TYPE_FOR_PLOTTING,
    DEFAULT_CHUNK_SIZE_FOR_PLOTTING,
    DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT,
    PLOT_DIR_CONFIG # Usar como fallback se não vier do pipeline
)

logger = logging.getLogger(__name__)

def run_chunk_evolution_visualization(
    db_manager: DatabaseManager,
    output_dir_from_pipeline: Optional[Path] = None, # Recebe o diretório do pipeline
    **kwargs # Aceita kwargs extras para compatibilidade
) -> bool:
    """
    Executa a visualização da evolução de métricas de chunks para configurações padrão.

    Args:
        db_manager: Instância do DatabaseManager.
        output_dir_from_pipeline: Diretório de saída para os gráficos, passado pelo pipeline.
                                  Se None, usa PLOT_DIR_CONFIG.
        **kwargs: Argumentos adicionais do pipeline (não usados aqui).

    Returns:
        True se a visualização foi gerada com sucesso, False caso contrário.
    """
    try:
        logger.info("Iniciando etapa: Visualização da Evolução de Métricas de Chunks.")
        
        # Define o diretório de saída
        # Prioriza o que foi passado pelo pipeline, caso contrário usa o default do config
        output_dir_to_use = output_dir_from_pipeline if output_dir_from_pipeline else PLOT_DIR_CONFIG
        
        # Garante que o diretório de saída exista (plot_chunk_metric_evolution também faz isso, mas é bom ter aqui)
        Path(output_dir_to_use).mkdir(parents=True, exist_ok=True)

        metric_to_plot = "Frequencia Absoluta" 

        logger.info(f"Gerando gráfico para: Tipo='{DEFAULT_CHUNK_TYPE_FOR_PLOTTING}', "
                    f"Tamanho={DEFAULT_CHUNK_SIZE_FOR_PLOTTING}, Métrica='{metric_to_plot}', "
                    f"Dezenas={DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT}, "
                    f"Output Dir='{str(output_dir_to_use)}'")
        
        plot_chunk_metric_evolution(
            db_manager=db_manager,
            chunk_type=DEFAULT_CHUNK_TYPE_FOR_PLOTTING,
            chunk_size=DEFAULT_CHUNK_SIZE_FOR_PLOTTING,
            metric_to_plot=metric_to_plot,
            dezenas_to_plot=DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT,
            output_dir=str(output_dir_to_use) # plot_chunk_metric_evolution espera string
        )
        
        logger.info("Etapa: Visualização da Evolução de Métricas de Chunks concluída com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa de visualização da evolução de métricas de chunks: {e}", exc_info=True)
        return False