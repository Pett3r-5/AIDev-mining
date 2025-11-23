import pandas as pd
from datetime import datetime
import numpy as np
import scipy.stats as stats

results = pd.read_csv('./results-v2.csv')
results = results[results['user_created_at'].notnull()]
all_users = pd.read_parquet("hf://datasets/hao-li/AIDev/all_user.parquet")
all_users_filtered = all_users[
    all_users["created_at"].notnull() &
    all_users["login"].notnull() &
    (all_users["login"] != "Copilot") & 
    ~all_users["login"].str.endswith('[bot]') &
    ~all_users["login"].str.endswith('bot') &
    ~all_users["login"].str.endswith('agent')
]

results["user_created_at"] = pd.to_datetime(results["user_created_at"].str.replace('T', ' ').str.replace('Z', '')).astype(np.int64) // 10**6
all_users_filtered["created_at"] = pd.to_datetime(all_users_filtered["created_at"].str.replace('T', ' ').str.replace('Z', '')).astype(np.int64) // 10**6

results = results.sort_values(by='user_created_at')
all_users_filtered = all_users_filtered.sort_values(by='created_at')
all_users_filtered = all_users_filtered.rename(columns={'created_at':'user_created_at'})
all_users_filtered = all_users_filtered[
    all_users_filtered["user_created_at"].notnull()
]

results = results[
    results["user_created_at"].notnull()
]

grupo_a_normalidade = stats.shapiro(results["user_created_at"].to_numpy())
print('Normalidade grupo A: ', f"{grupo_a_normalidade.pvalue:f}")

grupo_b_normalidade = stats.shapiro(all_users_filtered["user_created_at"].to_numpy())
print('Normalidade grupo B: ', f"{grupo_b_normalidade.pvalue:f}")
# grupo_b_normalidade = stats.anderson(all_users_filtered["user_created_at"].to_numpy())
# print(grupo_b_normalidade)

mannwhitneyuResult = stats.mannwhitneyu(results["user_created_at"].to_numpy(), all_users_filtered["user_created_at"].to_numpy())
print('Mann-Whitney U Resultado: ', f"{mannwhitneyuResult.pvalue:f}")
print('Mann-Whitney U Resultado: ', mannwhitneyuResult.pvalue)
