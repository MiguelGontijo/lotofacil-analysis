# src/analysis/grid_analysis.py
import pandas as pd
import logging
from typing import List, Dict, Any, Set
from collections import Counter

logger = logging.getLogger(__name__)

def analyze_grid_distribution(
    all_draws_df: pd.DataFrame, 
    config: Any
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Analisa a distribuição de quantas dezenas são sorteadas por linha e por coluna do volante.

    Args:
        all_draws_df (pd.DataFrame): DataFrame com todos os sorteios.
                                     Deve conter a coluna config.DRAWN_NUMBERS_COLUMN_NAME.
        config (Any): Objeto de configuração, que deve ter os atributos:
                      DRAWN_NUMBERS_COLUMN_NAME, LOTOFACIL_GRID_LINES,
                      LOTOFACIL_GRID_COLUMNS.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: 
            - line_distribution_df: DataFrame com a distribuição para as linhas.
                Colunas: 'Linha', 'Qtd_Dezenas_Sorteadas', 'Frequencia_Absoluta', 'Frequencia_Relativa'.
            - column_distribution_df: DataFrame com a distribuição para as colunas.
                Colunas: 'Coluna', 'Qtd_Dezenas_Sorteadas', 'Frequencia_Absoluta', 'Frequencia_Relativa'.
    """
    step_name = "Análise de Distribuição por Linhas e Colunas"
    logger.info(f"Iniciando {step_name}.")

    drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME
    grid_lines = config.LOTOFACIL_GRID_LINES
    grid_columns = config.LOTOFACIL_GRID_COLUMNS

    if drawn_numbers_col not in all_draws_df.columns:
        logger.error(f"Coluna '{drawn_numbers_col}' não encontrada no DataFrame de sorteios.")
        empty_df = pd.DataFrame(columns=['Elemento', 'Qtd_Dezenas_Sorteadas', 'Frequencia_Absoluta', 'Frequencia_Relativa'])
        return empty_df.rename(columns={'Elemento':'Linha'}), empty_df.rename(columns={'Elemento':'Coluna'})

    # Contadores para linhas e colunas
    # A chave será o nome da linha/coluna (ex: "L1", "C1")
    # O valor será um Counter para as quantidades (0 a 5 dezenas)
    line_counts_by_draw: Dict[str, List[int]] = {line_name: [] for line_name in grid_lines.keys()}
    column_counts_by_draw: Dict[str, List[int]] = {col_name: [] for col_name in grid_columns.keys()}

    total_draws = len(all_draws_df)
    if total_draws == 0:
        logger.warning("DataFrame de sorteios está vazio. Nenhuma análise de grid será realizada.")
        empty_df = pd.DataFrame(columns=['Elemento', 'Qtd_Dezenas_Sorteadas', 'Frequencia_Absoluta', 'Frequencia_Relativa'])
        return empty_df.rename(columns={'Elemento':'Linha'}), empty_df.rename(columns={'Elemento':'Coluna'})

    for _, row in all_draws_df.iterrows():
        drawn_numbers: Set[int] = set(row[drawn_numbers_col]) if isinstance(row[drawn_numbers_col], list) else set()

        # Contagem por Linhas
        for line_name, line_dezenas in grid_lines.items():
            count_in_line = len(drawn_numbers.intersection(set(line_dezenas)))
            line_counts_by_draw[line_name].append(count_in_line)

        # Contagem por Colunas
        for col_name, col_dezenas in grid_columns.items():
            count_in_col = len(drawn_numbers.intersection(set(col_dezenas)))
            column_counts_by_draw[col_name].append(count_in_col)

    # Processar e agregar resultados para Linhas
    line_distribution_results = []
    for line_name, counts_list in line_counts_by_draw.items():
        counts_summary = Counter(counts_list)
        for qtd_dezenas, freq_abs in counts_summary.items():
            freq_rel = freq_abs / total_draws if total_draws > 0 else 0.0
            line_distribution_results.append({
                'Linha': line_name,
                'Qtd_Dezenas_Sorteadas': qtd_dezenas,
                'Frequencia_Absoluta': freq_abs,
                'Frequencia_Relativa': round(freq_rel, 6)
            })
    line_distribution_df = pd.DataFrame(line_distribution_results)
    if not line_distribution_df.empty:
        line_distribution_df = line_distribution_df.sort_values(by=['Linha', 'Qtd_Dezenas_Sorteadas']).reset_index(drop=True)

    # Processar e agregar resultados para Colunas
    column_distribution_results = []
    for col_name, counts_list in column_counts_by_draw.items():
        counts_summary = Counter(counts_list)
        for qtd_dezenas, freq_abs in counts_summary.items():
            freq_rel = freq_abs / total_draws if total_draws > 0 else 0.0
            column_distribution_results.append({
                'Coluna': col_name,
                'Qtd_Dezenas_Sorteadas': qtd_dezenas,
                'Frequencia_Absoluta': freq_abs,
                'Frequencia_Relativa': round(freq_rel, 6)
            })
    column_distribution_df = pd.DataFrame(column_distribution_results)
    if not column_distribution_df.empty:
        column_distribution_df = column_distribution_df.sort_values(by=['Coluna', 'Qtd_Dezenas_Sorteadas']).reset_index(drop=True)
        
    logger.info(f"{step_name} concluída.")
    return line_distribution_df, column_distribution_df