import pandas as pd
import numpy as np
import math
import scipy.stats as stats

filtered_results = pd.read_csv('./results-v2.csv')
filtered_results = filtered_results[filtered_results['user_created_at'].notnull()]
# all_users = pd.read_parquet("hf://datasets/hao-li/AIDev/all_user.parquet")
all_commit_details = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")
pr_reviews = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_reviews.parquet")

all_commit_details_humans = all_commit_details[
    all_commit_details["author"].notnull()
]

all_commit_details_humans = all_commit_details_humans[
    (all_commit_details_humans["author"] != "Copilot") & 
    ~all_commit_details_humans["author"].str.endswith('[bot]') &
    ~all_commit_details_humans["author"].str.endswith('bot') &
    ~all_commit_details_humans["author"].str.endswith('agent')
]

pr_reviews_filtered_results = pr_reviews[
    pr_reviews['user'].isin(filtered_results["human_username"])
]

pr_reviews_total_population = pr_reviews[
    pr_reviews['user'].isin(all_commit_details_humans["author"])
]



# conta total de interaçoes em PR reviews por user
# possíveis interaçoes: COMMENTED, APPROVED, CHANGES_REQUESTED, DISMISSED
pr_count_filtered_results = pr_reviews_filtered_results['user'].value_counts()
pr_count_total_population = pr_reviews_total_population['user'].value_counts()


filtered_results_with_pr_info = pd.merge(filtered_results, pr_count_filtered_results, left_on="human_username", right_on="user", how='left')
all_commits_with_pr_info = pd.merge(all_commit_details_humans, pr_count_total_population, left_on="author", right_on="user", how='left')

all_commits_with_pr_info = all_commits_with_pr_info.drop_duplicates(subset=['author'])

# seta default de 0 para usuarios que nao tiveram nenhuma interaçao em PR
filtered_results_with_pr_info['count'] = filtered_results_with_pr_info['count'].apply(lambda count: 0 if math.isnan(count) else count)
all_commits_with_pr_info['count'] = all_commits_with_pr_info['count'].apply(lambda count: 0 if math.isnan(count) else count)

# results["user_created_at"] = pd.to_datetime(results["user_created_at"].str.replace('T', ' ').str.replace('Z', '')).astype(np.int64) // 10**6
# all_users_filtered["created_at"] = pd.to_datetime(all_users_filtered["created_at"].str.replace('T', ' ').str.replace('Z', '')).astype(np.int64) // 10**6

# filtered_results = filtered_results.sort_values(by='user_created_at')
# all_commit_details_humans = all_commit_details_humans.sort_values(by='created_at')
# all_commit_details_humans = all_commit_details_humans.rename(columns={'created_at':'user_created_at'})
# all_commit_details_humans = all_commit_details_humans[
#     all_commit_details_humans["user_created_at"].notnull()
# ]

# filtered_results = filtered_results[
#     filtered_results["user_created_at"].notnull()
# ]

filtered_results_with_pr_info["count"] = filtered_results_with_pr_info["count"].astype(int)
all_commits_with_pr_info["count"] = all_commits_with_pr_info["count"].astype(int)
filtered_results_with_pr_info = filtered_results_with_pr_info.sort_values(by='count')
all_commits_with_pr_info = all_commits_with_pr_info.sort_values(by='count')
all_commits_with_pr_info = all_commits_with_pr_info[["sha", "author", "filename", "status", "count"]]

filtered_results_with_pr_info = filtered_results_with_pr_info.rename(columns={'count':'pr_review_count'})
all_commits_with_pr_info = all_commits_with_pr_info.rename(columns={'count':'pr_review_count'})

#debug
filtered_results_with_pr_info.to_csv("selected-users-count.csv")
all_commits_with_pr_info.to_csv("all-users-count.csv")


grupo_a_normalidade = stats.shapiro(filtered_results_with_pr_info["pr_review_count"].to_numpy())
print('Normalidade grupo A: ', f"{grupo_a_normalidade.pvalue:f}")
print('Normalidade grupo A: ', grupo_a_normalidade.pvalue)

grupo_b_normalidade = stats.shapiro(all_commits_with_pr_info["pr_review_count"].to_numpy())
print('Normalidade grupo B: ', f"{grupo_b_normalidade.pvalue:f}")
print('Normalidade grupo B: ', grupo_b_normalidade.pvalue)
# grupo_b_normalidade = stats.anderson(all_users_filtered["user_created_at"].to_numpy())
# print(grupo_b_normalidade)

mannwhitneyuResult = stats.mannwhitneyu(filtered_results_with_pr_info["pr_review_count"].to_numpy(), all_commits_with_pr_info["pr_review_count"].to_numpy())
print('Mann-Whitney U Resultado: ', f"{mannwhitneyuResult.pvalue:f}")
print('Mann-Whitney U Resultado: ', mannwhitneyuResult.pvalue)
