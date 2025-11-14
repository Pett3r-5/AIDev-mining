import pandas as pd

all_commit_details = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")
all_users = pd.read_parquet("hf://datasets/hao-li/AIDev/user.parquet")

notnull_commits = all_commit_details[all_commit_details["author"].notnull() & all_commit_details["filename"].notnull()
                                     & all_commit_details["message"].notnull()]

human_commits = notnull_commits[
    (notnull_commits["author"] != "Copilot") & 
    ~notnull_commits["author"].str.endswith('[bot]') &
    ~notnull_commits["author"].str.endswith('bot') &
    (notnull_commits["author"] != 'cursoragent')]



human_test_commits = human_commits[
        human_commits["filename"].str.contains('test') &
        ~human_commits["message"].str.startswith('Merge branch')
    ]

#debug
# human_test_commits["filename"].to_csv("all-tests.csv")

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
human_test_pull_commits["debug-origin"] = None

bot_pull_request_commits = pd.merge(bot_commits, all_pull_requests, on="pr_id", how='inner')


bot_pull_request_commits = bot_pull_request_commits[bot_pull_request_commits["repo_url"].isin(human_test_pull_commits["repo_url"].to_numpy())]


bot_pull_request_commits["filename.test"] = None
bot_pull_request_commits["filename.spec"] = None
bot_pull_request_commits["filename_spec"] = None
bot_pull_request_commits["filename-underscore-test"] = None
bot_pull_request_commits["filename-endswith-test"] = None
bot_pull_request_commits["filename-startswith-test"] = None
bot_pull_request_commits["filename-startswith-underscore-test"] = None
bot_pull_request_commits["filename-test-folder"] = None
bot_pull_request_commits["original-filename"] = bot_pull_request_commits["filename"]

# pra cada filename, insere um filename respectivo de teste, com cada test naming pattern, 
# pra depois poder fazer um join com os commits humanos que tenham filenames de teste
for index, row in bot_pull_request_commits.iterrows():
    filename_without_path = row["filename"].split('/')[-1]
    extension = filename_without_path.split('.')[-1]
    bot_pull_request_commits.at[index, "filename.test"] = filename_without_path.split('.' + extension)[0] + '.test' + '.' + extension
    bot_pull_request_commits.at[index, "filename.spec"] = filename_without_path.split('.' + extension)[0] + '.spec' + '.' + extension
    bot_pull_request_commits.at[index, "filename_spec"] = filename_without_path.split('.' + extension)[0] + '_spec' + '.' + extension
    bot_pull_request_commits.at[index, "filename-underscore-test"] = filename_without_path.split('.' + extension)[0] + '_test.' + extension
    bot_pull_request_commits.at[index, "filename-endswith-test"] = filename_without_path.split('.' + extension)[0] + 'Test.' + extension
    bot_pull_request_commits.at[index, "filename-startswith-test"] = 'Test' + filename_without_path
    bot_pull_request_commits.at[index, "filename-startswith-underscore-test"] = 'test_' + filename_without_path

    if (not row["filename"].startswith('test')) and (not 'src/test' in row["filename"]) :
        filename_without_path = row["filename"].split('/')[-1]
        bot_pull_request_commits.at[index, "filename-test-folder"] = filename_without_path  



test_root_dir_human_commits = human_test_pull_commits[
        ((human_test_pull_commits["filename"].str.startswith('test')) &
         (human_test_pull_commits["filename"].str.contains('/'))
         ) |
        (human_test_pull_commits["filename"].str.contains('src/test/')) |
        (human_test_pull_commits["filename"].str.contains('src/tests/'))
].copy()


# arquivos de teste e arquivos testados podem estar em um path diferente (ex pasta /tests),
# entao só comparamos o nome do arquivo, e nao o caminho completo
human_test_pull_commits["filename"] = human_test_pull_commits["filename"].str.rsplit('/').str[-1]
test_root_dir_human_commits["filename"] = test_root_dir_human_commits["filename"].str.rsplit('/').str[-1]

# final_human_commits = human_test_pull_commits[(
#         (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename.spec"])) |
#         (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename_spec"])) |
#          (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename.test"])) |
#          (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-underscore-test"])) |
#          (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-endswith-test"])) |
#          (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-startswith-test"])) |
#          (human_test_pull_commits["filename"].isin(bot_pull_request_commits["filename-startswith-underscore-test"]))
#     )]

final_human_commits = human_test_pull_commits


human_specFiles = final_human_commits.rename(columns={'filename':'filename.spec'})
human_specFiles["debug-origin"] = 'filename.spec'

human_UnderscoreSpecFiles = final_human_commits.rename(columns={'filename':'filename_spec'})
human_UnderscoreSpecFiles["debug-origin"] = 'filename_spec'

human_testFiles = final_human_commits.rename(columns={'filename':'filename.test'})
human_testFiles["debug-origin"] = 'filename.test'

human_underscoreTestFiles = final_human_commits.rename(columns={'filename':'filename-underscore-test'})
human_underscoreTestFiles["debug-origin"] = 'filename-underscore-test'

human_endswithTestFiles = final_human_commits.rename(columns={'filename':'filename-endswith-test'})
human_endswithTestFiles["debug-origin"] = 'filename-endswith-test'

human_startswithTestFiles = final_human_commits.rename(columns={'filename':'filename-startswith-test'})
human_startswithTestFiles["debug-origin"] = 'filename-startswith-test'

human_startswithUnderscoreTestFiles = final_human_commits.rename(columns={'filename':'filename-startswith-underscore-test'})
human_startswithUnderscoreTestFiles["debug-origin"] = 'filename-startswith-underscore-test'

human_testFolders = test_root_dir_human_commits.rename(columns={'filename':'filename-test-folder'})
human_testFolders["debug-origin"] = 'filename-test-folder'


specFiles = pd.merge(human_specFiles, bot_pull_request_commits, left_on=["filename.spec", "repo_url"], right_on=["filename.spec", "repo_url"], how='inner')
underscoreSpecFiles = pd.merge(human_UnderscoreSpecFiles, bot_pull_request_commits, left_on=["filename_spec", "repo_url"], right_on=["filename_spec", "repo_url"], how='inner')
testFiles = pd.merge(human_testFiles, bot_pull_request_commits, left_on=["filename.test", "repo_url"], right_on=["filename.test", "repo_url"], how='inner')
underscoreTestFiles = pd.merge(human_underscoreTestFiles, bot_pull_request_commits, left_on=["filename-underscore-test", "repo_url"], right_on=["filename-underscore-test", "repo_url"], how='inner')
endswithTestFiles = pd.merge(human_endswithTestFiles, bot_pull_request_commits, left_on=["filename-endswith-test", "repo_url"], right_on=["filename-endswith-test", "repo_url"], how='inner')
startswithTestFiles = pd.merge(human_startswithTestFiles, bot_pull_request_commits, left_on=["filename-startswith-test", "repo_url"], right_on=["filename-startswith-test", "repo_url"], how='inner')
startswithUnderscoreTestFiles = pd.merge(human_startswithUnderscoreTestFiles, bot_pull_request_commits, left_on=["filename-startswith-underscore-test", "repo_url"], right_on=["filename-startswith-test", "repo_url"], how='inner')
testFolders = pd.merge(human_testFolders, bot_pull_request_commits, left_on=["filename-test-folder", "repo_url"], right_on=["filename-test-folder", "repo_url"], how='inner')

final_dataframe = pd.concat([specFiles, underscoreSpecFiles, testFiles, underscoreTestFiles, endswithTestFiles, startswithTestFiles, testFolders, startswithUnderscoreTestFiles])
final_dataframe = final_dataframe[(final_dataframe["created_at_x"] > final_dataframe["merged_at_y"]) | (final_dataframe["pr_id_x"] == final_dataframe["pr_id_y"])].drop_duplicates(subset=['author_x'])


all_users = all_users.rename(columns={'login':'author_x'})
all_users = all_users.rename(columns={'created_at':'user_created_at'})

final_dataframe_with_users = pd.merge(final_dataframe, all_users, on="author_x", how='inner').sort_values(by='user_created_at')


final_dataframe_with_users[["html_url_x", "html_url_y", "sha_x", "sha_y", "author_x", "author_y", "original-filename", "user_created_at", "debug-origin"]].to_csv("results-v2.csv")