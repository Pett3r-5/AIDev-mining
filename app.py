import pandas as pd

all_commit_details = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")
all_users = pd.read_parquet("hf://datasets/hao-li/AIDev/user.parquet")

notnull_commits = all_commit_details[all_commit_details["author"].notnull() & all_commit_details["filename"].notnull()]

human_commits = notnull_commits[
    (notnull_commits["author"] != "Copilot") & 
    ~notnull_commits["author"].str.endswith('[bot]') &
    ~notnull_commits["author"].str.endswith('bot') &
    (notnull_commits["author"] != 'cursoragent')]

# file_extensions = human_commits[human_commits["filename"].str.contains(".")]
# file_extensions["filename"] = file_extensions["filename"].str.rsplit("/").str[-1].str.rsplit(".", n=1).str[-1]
# file_extensions["filename"].drop_duplicates().to_csv("all-file-extensions.csv")

# print("total tests count", len(human_commits[human_commits["filename"].str.contains("test")]))
# human_commits[human_commits["filename"].str.contains("test")]["filename"].to_csv("all_tests.csv")

# print("java count", len(human_commits[human_commits["filename"].str.endswith('Test.java') |
#           (
#               human_commits["filename"].str.endswith('.java') & human_commits["filename"].str.startswith('Test')
#             #    human_commits["filename"].str.endswith('.java') & human_commits["filename"].str.split('/')[-1].startswith('Test')
#            )
#            ]))

# print("python count", len(human_commits[human_commits["filename"].str.endswith('test.py') |
#           (
#               human_commits["filename"].str.endswith('.py') & human_commits["filename"].str.startswith('test_')
#               )
#               ]))

# print("js count", len(human_commits[human_commits["filename"].str.endswith('.test.js') |
#           human_commits["filename"].str.endswith('.test.jsx') |
#           human_commits["filename"].str.endswith('.spec.js') |
#           human_commits["filename"].str.endswith('.spec.jsx')
#           ]))

# print("ts count", len(human_commits[human_commits["filename"].str.endswith('.test.ts') |
#           human_commits["filename"].str.endswith('.test.tsx') |
#           human_commits["filename"].str.endswith('.spec.ts') |
#           human_commits["filename"].str.endswith('.spec.tsx')
#           ]))

human_test_commits = human_commits[
        human_commits["filename"].str.endswith('test.ts') |
        human_commits["filename"].str.endswith('test.tsx') |
        human_commits["filename"].str.endswith('spec.ts') |
        human_commits["filename"].str.endswith('spec.tsx')
    ]

bot_commits = notnull_commits[
    (notnull_commits["author"] == "Copilot") |
    notnull_commits["author"].str.endswith('[bot]') |
    notnull_commits["author"].str.endswith('bot') |
    (notnull_commits["author"] == 'cursoragent')
    ]


all_pull_requests = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
all_pull_requests = all_pull_requests[all_pull_requests["repo_url"].notnull() & 
                                      all_pull_requests["created_at"].notnull() &
                                      all_pull_requests["merged_at"].notnull()
                                      ]
all_pull_requests.rename(columns={'id':'pr_id'}, inplace = True)
human_test_pull_commits = pd.merge(human_test_commits, all_pull_requests, on="pr_id", how='inner')
bot_pull_request_commits = pd.merge(bot_commits, all_pull_requests, on="pr_id", how='inner')


bot_pull_request_commits = bot_pull_request_commits[bot_pull_request_commits["repo_url"].isin(human_test_pull_commits["repo_url"].to_numpy())]
bot_pull_request_commits["filename-test"] = None
bot_pull_request_commits["filename-spec"] = None


for index, row in bot_pull_request_commits.iterrows():
    if row["filename"].endswith('.tsx'):
        bot_pull_request_commits.at[index, "filename-test"] = row["filename"].split('.tsx')[0] + '.test.tsx'
        bot_pull_request_commits.at[index, "filename-spec"] = row["filename"].split('.tsx')[0] + '.spec.tsx'

    if row["filename"].endswith('.ts'):
        bot_pull_request_commits.at[index, "filename-test"] = row["filename"].split('.ts')[0] + '.test.ts'
        bot_pull_request_commits.at[index, "filename-spec"] = row["filename"].split('.ts')[0] + '.spec.ts'


bot_pull_request_commits = bot_pull_request_commits[bot_pull_request_commits["filename-test"].notnull() & bot_pull_request_commits["filename-spec"].notnull()]


final_commits = human_test_pull_commits[(
        (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-spec"])) |
         (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-test"]))
    )]


specFiles = final_commits.rename(columns={'filename':'filename-spec'})
testFiles = final_commits.rename(columns={'filename':'filename-test'})


specFiles = pd.merge(specFiles, bot_pull_request_commits, left_on=["filename-spec", "repo_url"], right_on=["filename-spec", "repo_url"], how='inner')
testFiles = pd.merge(testFiles, bot_pull_request_commits, left_on=["filename-test", "repo_url"], right_on=["filename-test", "repo_url"], how='inner')

final_dataframe = pd.concat([specFiles, testFiles])
final_dataframe = final_dataframe[(final_dataframe["created_at_x"] > final_dataframe["merged_at_y"]) | (final_dataframe["pr_id_x"] == final_dataframe["pr_id_y"])].drop_duplicates(subset=['author_x'])


all_users = all_users.rename(columns={'login':'author_x'})
all_users = all_users.rename(columns={'created_at':'user_created_at'})

final_dataframe_with_users = pd.merge(final_dataframe, all_users, on="author_x", how='inner')


final_dataframe_with_users[["sha_x", "author_x", "html_url_x", "sha_y", "author_y", "html_url_y", "filename-spec", "user_created_at"]].to_csv("results.csv")

unique_users = {}
# for username in test_commits_for_bot_correction["human_author"]:
#     if username in unique_users:
#         unique_users[username]["commits_count"] += 1
#     else:
#         unique_users[username] = {}
#         unique_users[username]["commits_count"] = 1


# users_detail = pd.read_parquet("hf://datasets/hao-li/AIDev/user.parquet")

# invalid_users = []
# for key in unique_users:
#     user_detail = users_detail[users_detail["login"] == key]
    
#     if not user_detail["created_at"].empty:
#         unique_users[key]["created_at"] = user_detail["created_at"].values[0]
#     else:
#         print("invalid user")
#         print(key)
#         print("\n")
#         invalid_users.append(key)

# for invalid_user in invalid_users:
#     del unique_users[invalid_user]

# print("unique_users: ", unique_users)
# print("\n")
# print("Length: ", len(unique_users))