import pandas as pd

all_commit_details = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")
all_users = pd.read_parquet("hf://datasets/hao-li/AIDev/user.parquet")

notnull_commits = all_commit_details[all_commit_details["author"].notnull() & all_commit_details["filename"].notnull()]

human_commits = notnull_commits[
    (notnull_commits["author"] != "Copilot") & 
    ~notnull_commits["author"].str.endswith('[bot]') &
    ~notnull_commits["author"].str.endswith('bot') &
    (notnull_commits["author"] != 'cursoragent')]


# naming patterns de arquivos de teste pra principais linguagens
human_test_commits = human_commits[
        human_commits["filename"].str.endswith('test.ts') |
        human_commits["filename"].str.endswith('test.tsx') |
        human_commits["filename"].str.endswith('spec.ts') |
        human_commits["filename"].str.endswith('spec.tsx') |
        human_commits["filename"].str.endswith('test.js') |
        human_commits["filename"].str.endswith('test.jsx') |
        human_commits["filename"].str.endswith('spec.js') |
        human_commits["filename"].str.endswith('spec.jsx') |
        human_commits["filename"].str.endswith('Test.java') |
        (
            human_commits["filename"].str.rsplit("/").str[-1].str.startswith('Test') &
            human_commits["filename"].str.endswith('.java')
         ) |
        human_commits["filename"].str.endswith('test.py') |
        human_commits["filename"].str.endswith('_test.go') |
        human_commits["filename"].str.endswith('test.cs') |
        (
            human_commits["filename"].str.startswith('tests/') &
            human_commits["filename"].str.endswith('.cs')
         ) |
         (
            human_commits["filename"].str.rsplit("/").str[-1].str.startswith('Test') &
            human_commits["filename"].str.endswith('.cs')
         )
    ]

#debug
human_test_commits["filename"].to_csv("all-tests.csv")

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
bot_pull_request_commits["filename-underscore-test"] = None
bot_pull_request_commits["filename-endswith-test"] = None
bot_pull_request_commits["filename-startswith-test"] = None
bot_pull_request_commits["filename-test-folder-cs"] = None

# pra cada filename, insere um filename respectivo de teste, com cada test naming pattern, 
# pra depois poder fazer um join com os commits humanos que tenham filenames de teste
for index, row in bot_pull_request_commits.iterrows():
    if (row["filename"].endswith('.tsx') or 
        row["filename"].endswith('.ts') or
        row["filename"].endswith('.jsx') or
        row["filename"].endswith('.js')):
        filename_without_path = row["filename"].split('/')[-1]
        extension = filename_without_path.split('.')[-1]
        bot_pull_request_commits.at[index, "filename-test"] = filename_without_path.split('.' + extension)[0] + '.test' + '.' + extension
        bot_pull_request_commits.at[index, "filename-spec"] = filename_without_path.split('.' + extension)[0] + '.spec' + '.' + extension

    if row["filename"].endswith('.go') or row["filename"].endswith('.py'):
        filename_without_path = row["filename"].split('/')[-1]
        extension = filename_without_path.split('.')[-1]
        bot_pull_request_commits.at[index, "filename-underscore-test"] = filename_without_path.split('.' + extension)[0] + '_test.' + extension

    if row["filename"].endswith('.java') or row["filename"].endswith('.py') or row["filename"].endswith('.cs'):
        filename_without_path = row["filename"].split('/')[-1]
        extension = filename_without_path.split('.')[-1]
        bot_pull_request_commits.at[index, "filename-endswith-test"] = filename_without_path.split('.' + extension)[0] + 'Test.' + extension

    if row["filename"].endswith('.java') or row["filename"].endswith('.cs'):
        filename_without_path = row["filename"].split('/')[-1]
        bot_pull_request_commits.at[index, "filename-startswith-test"] = 'Test' + filename_without_path

    if row["filename"].endswith('.cs') and not row["filename"].startswith('tests/'):
        filename_without_path = row["filename"].split('/')[-1]
        bot_pull_request_commits.at[index, "filename-test-folder-cs"] = filename_without_path      





human_test_pull_commits["filename"] = human_test_pull_commits["filename"].str.rsplit('/').str[-1]

final_human_commits = human_test_pull_commits[(
        (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-spec"])) |
         (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-test"])) |
         (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-underscore-test"])) |
         (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-endswith-test"])) |
         (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-startswith-test"])) |
         (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-test-folder-cs"]))
    )]


human_specFiles = final_human_commits.rename(columns={'filename':'filename-spec'})
human_testFiles = final_human_commits.rename(columns={'filename':'filename-test'})
human_underscoreTestFiles = final_human_commits.rename(columns={'filename':'filename-underscore-test'})
human_endswithTestFiles = final_human_commits.rename(columns={'filename':'filename-endswith-test'})
human_startswithTestFiles = final_human_commits.rename(columns={'filename':'filename-startswith-test'})
human_testFolders = final_human_commits.rename(columns={'filename':'filename-test-folder-cs'})


specFiles = pd.merge(human_specFiles, bot_pull_request_commits, left_on=["filename-spec", "repo_url"], right_on=["filename-spec", "repo_url"], how='inner')
testFiles = pd.merge(human_testFiles, bot_pull_request_commits, left_on=["filename-test", "repo_url"], right_on=["filename-test", "repo_url"], how='inner')
underscoreTestFiles = pd.merge(human_underscoreTestFiles, bot_pull_request_commits, left_on=["filename-underscore-test", "repo_url"], right_on=["filename-underscore-test", "repo_url"], how='inner')
endswithTestFiles = pd.merge(human_endswithTestFiles, bot_pull_request_commits, left_on=["filename-endswith-test", "repo_url"], right_on=["filename-endswith-test", "repo_url"], how='inner')
startswithTestFiles = pd.merge(human_startswithTestFiles, bot_pull_request_commits, left_on=["filename-startswith-test", "repo_url"], right_on=["filename-startswith-test", "repo_url"], how='inner')
testFolders = pd.merge(human_testFolders, bot_pull_request_commits, left_on=["filename-test-folder-cs", "repo_url"], right_on=["filename-test-folder-cs", "repo_url"], how='inner')

final_dataframe = pd.concat([specFiles, testFiles, underscoreTestFiles, endswithTestFiles, startswithTestFiles, testFolders])
final_dataframe = final_dataframe[(final_dataframe["created_at_x"] > final_dataframe["merged_at_y"]) | (final_dataframe["pr_id_x"] == final_dataframe["pr_id_y"])].drop_duplicates(subset=['author_x'])


all_users = all_users.rename(columns={'login':'author_x'})
all_users = all_users.rename(columns={'created_at':'user_created_at'})

final_dataframe_with_users = pd.merge(final_dataframe, all_users, on="author_x", how='inner').sort_values(by='user_created_at')


final_dataframe_with_users[["sha_x", "author_x", "html_url_x", "sha_y", "author_y", "html_url_y", "filename-spec", "user_created_at"]].to_csv("results.csv")