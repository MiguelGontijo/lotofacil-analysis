# src/pipeline_steps/execute_properties.py

import argparse
from src.config import logger
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties
try:
    from src.visualization.plotter import plot_distribution_bar
    PLOTTING_ENABLED_STEP = True
except ImportError:
    PLOTTING_ENABLED_STEP = False


def execute_properties_analysis(args: argparse.Namespace, should_plot: bool):
    """ Executa e exibe a análise de propriedades dos números. """
    logger.info(f"Executando análise de propriedades...")
    max_c = args.max_concurso
    plot_flag = should_plot and PLOTTING_ENABLED_STEP

    properties_df = analyze_number_properties(concurso_maximo=max_c)
    if properties_df is not None and not properties_df.empty:
        prop_summaries = summarize_properties(properties_df)
        for key, series in prop_summaries.items():
            title = key.replace('_', ' ').replace('par impar', 'Pares/Ímpares').replace('moldura miolo', 'Moldura/Miolo').title()
            print(f"\n--- Frequência {title} ---")
            print(series.to_string())
            if plot_flag:
                plot_distribution_bar(series, title, f"dist_{key}")
    else:
         logger.error("Falha ao analisar as propriedades dos números.")
    logger.info("Análise de propriedades concluída.")