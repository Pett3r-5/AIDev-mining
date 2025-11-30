import scipy.stats as stats

def calculate_stats(filtered_results_with_pr_info, all_commits_with_pr_info):
    grupo_a_normalidade = stats.shapiro(filtered_results_with_pr_info["pr_review_count"].to_numpy())
    print('Normalidade grupo A: ', f"{grupo_a_normalidade.pvalue:10f}")
    print('Normalidade grupo A: ', grupo_a_normalidade.pvalue)

    grupo_b_normalidade = stats.shapiro(all_commits_with_pr_info["pr_review_count"].to_numpy())
    print('Normalidade grupo B: ', f"{grupo_b_normalidade.pvalue:10f}")
    print('Normalidade grupo B: ', grupo_b_normalidade.pvalue)

    mannwhitneyuResult = stats.mannwhitneyu(filtered_results_with_pr_info["pr_review_count"].to_numpy(), all_commits_with_pr_info["pr_review_count"].to_numpy())
    print('Mann-Whitney U Resultado: ', f"{mannwhitneyuResult.pvalue:10f}")
    print('Mann-Whitney U Resultado: ', mannwhitneyuResult.pvalue)

