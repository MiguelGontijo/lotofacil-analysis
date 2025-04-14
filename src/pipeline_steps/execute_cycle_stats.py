# src/pipeline_steps/execute_cycle_stats.py

import pandas as pd
from typing import Dict, Optional, Tuple # Adicionado Tuple
from src.config import logger
# Importa a NOVA função que retorna o dicionário de frequências
from src.analysis.cycle_analysis import calculate_frequency_per_cycle

def execute_cycle_stats_analysis(cycles_summary: Optional[pd.DataFrame]):
    """
    Calcula a frequência por ciclo e exibe um resumo para ciclos selecionados.
    """
    logger.info("Executando análise de stats por ciclo...")
    if cycles_summary is None or cycles_summary.empty:
        logger.warning("DataFrame de ciclos vazio ou None. Análise de stats por ciclo não realizada.")
        return

    # Chama a função que calcula a frequência para TODOS os ciclos e RETORNA o dicionário
    cycle_freq_dict: Dict[int, Optional[pd.Series]] = calculate_frequency_per_cycle(cycles_summary)

    if not cycle_freq_dict:
        logger.warning("Nenhuma frequência por ciclo foi calculada.")
        return

    # --- Lógica para selecionar e exibir ciclos específicos ---
    num_cycles_each_end = 3
    # Dicionário para guardar {nome_ciclo: numero_ciclo} para exibição
    cycles_to_display_map: Dict[str, int] = {}
    total_cycles = len(cycles_summary)

    # Adiciona primeiros N ao mapa
    if total_cycles > 0:
        for i in range(min(num_cycles_each_end, total_cycles)):
            row = cycles_summary.iloc[i]
            cycle_num = int(row['numero_ciclo'])
            name = f"Ciclo {cycle_num} ({int(row['duracao'])} conc.) [{int(row['concurso_inicio'])}-{int(row['concurso_fim'])}]"
            cycles_to_display_map[name] = cycle_num

    # Adiciona últimos N ao mapa
    if total_cycles > num_cycles_each_end:
         start_idx = max(num_cycles_each_end, total_cycles - num_cycles_each_end)
         for i in range(total_cycles - 1, start_idx - 1, -1):
             row = cycles_summary.iloc[i]
             cycle_num = int(row['numero_ciclo'])
             name = f"Ciclo {cycle_num} ({int(row['duracao'])} conc.) [{int(row['concurso_inicio'])}-{int(row['concurso_fim'])}]"
             # Adiciona apenas se ainda não estiver (evita duplicar se N for grande)
             if name not in cycles_to_display_map and cycle_num not in cycles_to_display_map.values():
                cycles_to_display_map[name] = cycle_num

    # Adiciona ciclos extremos ao mapa
    try:
        shortest_cycle = cycles_summary.loc[cycles_summary['duracao'].idxmin()]
        short_num = int(shortest_cycle['numero_ciclo'])
        name_short = f"Ciclo Mais Curto ({int(shortest_cycle['duracao'])} conc. - nº{short_num}) [{int(shortest_cycle['concurso_inicio'])}-{int(shortest_cycle['concurso_fim'])}]"
        if name_short not in cycles_to_display_map and short_num not in cycles_to_display_map.values():
            cycles_to_display_map[name_short] = short_num

        longest_cycle = cycles_summary.loc[cycles_summary['duracao'].idxmax()]
        long_num = int(longest_cycle['numero_ciclo'])
        name_long = f"Ciclo Mais Longo ({int(longest_cycle['duracao'])} conc. - nº{long_num}) [{int(longest_cycle['concurso_inicio'])}-{int(longest_cycle['concurso_fim'])}]"
        if name_long not in cycles_to_display_map and long_num not in cycles_to_display_map.values():
            cycles_to_display_map[name_long] = long_num
    except ValueError:
        logger.warning("Não foi possível determinar ciclos extremos para exibição.")

    # Imprime os resultados para os ciclos selecionados
    print("\n--- Análise de Frequência Dentro de Ciclos Selecionados ---")
    # Itera sobre os ciclos selecionados para exibição
    # (Ordenar pelo número do ciclo pode ser melhor que pela string do nome)
    # Criar lista de tuplas (numero_ciclo, nome_ciclo) e ordenar
    display_items = sorted([(num, nome) for nome, num in cycles_to_display_map.items()])

    for cycle_num, cycle_name in display_items:
        # Pega a Series de frequência do dicionário calculado
        freq_series = cycle_freq_dict.get(cycle_num)
        if freq_series is not None and not freq_series.empty:
            print(f"\n>> Frequência no {cycle_name} <<")
            print("Top 5 Mais Frequentes:")
            print(freq_series.nlargest(5).to_string())
            print("\nTop 5 Menos Frequentes:")
            print(freq_series.nsmallest(5).to_string())
            print("-" * 30)
        else:
            logger.warning(f"Frequência não encontrada ou vazia para o ciclo {cycle_num} ({cycle_name}) no dicionário.")

    logger.info("Análise de stats por ciclo concluída (resumo impresso).")