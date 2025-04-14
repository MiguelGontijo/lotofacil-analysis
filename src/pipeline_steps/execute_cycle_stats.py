# src/pipeline_steps/execute_cycle_stats.py

import pandas as pd
from typing import Optional
import argparse # <<< IMPORT ADICIONADO AQUI >>>
from src.config import logger
# Importa a função que LÊ ciclos e a que CALCULA/IMPRIME freq por ciclo
from src.analysis.cycle_analysis import get_cycles_df, run_cycle_frequency_analysis

# A assinatura da função agora funciona pois argparse foi importado
def execute_cycle_stats_analysis(cycles_summary: Optional[pd.DataFrame], args: argparse.Namespace): # Recebe args
    """
    Lê os ciclos da tabela (se necessário, embora geralmente já venham do orchestrator)
    e executa a análise de frequência dentro deles.
    Nota: Atualmente run_cycle_frequency_analysis imprime diretamente.
    """
    logger.info("Executando análise de stats por ciclo...")

    # Verifica se recebeu um DataFrame válido de ciclos
    if cycles_summary is None or cycles_summary.empty:
        logger.warning("DataFrame de ciclos vazio ou None passado para execute_cycle_stats_analysis. Análise não realizada.")
        # Tenta ler do DB como fallback? Ou confia no orchestrator?
        # Vamos confiar no orchestrator por enquanto.
        # cycles_summary = get_cycles_df(concurso_maximo=args.max_concurso) # Opção de fallback
        # if cycles_summary is None or cycles_summary.empty:
        #     logger.error("Falha ao ler ciclos da tabela como fallback.")
        #     return
        return # Simplesmente retorna se não recebeu ciclos válidos

    # Chama a função que calcula e imprime as frequências para os ciclos fornecidos
    # (run_cycle_frequency_analysis já contém a lógica de seleção e impressão)
    run_cycle_frequency_analysis(cycles_summary)
    # O log de conclusão já está dentro dela