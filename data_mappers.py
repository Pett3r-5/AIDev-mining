import pandas as pd
import math
import requests

def build_human_and_bot_commit_data():
    all_commit_details = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")
    all_users = pd.read_parquet("hf://datasets/hao-li/AIDev/all_user.parquet")

    notnull_commits = all_commit_details[all_commit_details["author"].notnull() &
                                         all_commit_details["filename"].notnull() &
                                        all_commit_details["message"].notnull()]
    
    all_human_commits = notnull_commits[
        (notnull_commits["author"] != "Copilot") & 
        ~notnull_commits["author"].str.endswith('[bot]') &
        ~notnull_commits["author"].str.endswith('bot') &
        ~notnull_commits["author"].str.endswith('agent')
        ]

    general_population_final = all_human_commits
    general_population_final = general_population_final.drop_duplicates(subset=['author'])



    human_test_commits = all_human_commits[
            (all_human_commits["filename"].str.contains('test') |
            all_human_commits["filename"].str.contains('spec')
            ) &
            ~all_human_commits["message"].str.startswith('Merge ')
        ]

    #debug
    human_test_commits["filename"].to_csv("all-tests.csv")

    bot_commits = notnull_commits[
        (notnull_commits["author"] == "Copilot") |
        notnull_commits["author"].str.endswith('[bot]') |
        notnull_commits["author"].str.endswith('bot') |
        notnull_commits["author"].str.endswith('agent')
        ]


    all_pull_requests = pd.read_parquet("hf://datasets/hao-li/AIDev/all_pull_request.parquet")
    all_pull_requests = all_pull_requests[all_pull_requests["repo_url"].notnull() & 
                                        all_pull_requests["created_at"].notnull()
                                        ]

    pr_timeline = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_timeline.parquet")
    pr_timeline = pr_timeline[pr_timeline["created_at"].notnull() & 
                                        pr_timeline["commit_id"].notnull()
                                        ]
    pr_timeline = pr_timeline.rename(columns={'created_at':'commit_created_at'})

    human_test_pull_commits = pd.merge(human_test_commits, all_pull_requests, left_on="pr_id", right_on="id", how='inner')
    human_test_pull_commits = pd.merge(human_test_pull_commits, pr_timeline, left_on="sha", right_on="commit_id", how='left')
 

    bot_pull_request_commits = pd.merge(bot_commits, all_pull_requests, left_on="pr_id", right_on="id", how='inner')
    bot_pull_request_commits = pd.merge(bot_pull_request_commits, pr_timeline, left_on="sha", right_on="commit_id", how='left')

    return {
        "all_users": all_users,
        "general_population_final": general_population_final,
        "bot_pull_request_commits": bot_pull_request_commits,
        "human_test_pull_commits": human_test_pull_commits
    }



def map_test_files_to_tested_files(bot_pull_request_commits, human_test_pull_commits):
    bot_pull_request_commits = bot_pull_request_commits[bot_pull_request_commits["repo_url"].isin(human_test_pull_commits["repo_url"].to_numpy())]

    human_test_pull_commits["test-filename-pattern"] = None

    bot_pull_request_commits["*filename*.test"] = None
    bot_pull_request_commits["*filename*.spec"] = None
    bot_pull_request_commits["*filename*_spec"] = None
    bot_pull_request_commits["*filename*_test"] = None
    bot_pull_request_commits["*filename*_tests"] = None
    bot_pull_request_commits["*filename*Test"] = None
    bot_pull_request_commits["*filename*Tests"] = None
    bot_pull_request_commits["Test*filename*"] = None
    bot_pull_request_commits["test_*filename*"] = None
    bot_pull_request_commits["filename-test-folder"] = None
    bot_pull_request_commits.loc[:, "original-filename"] = bot_pull_request_commits["filename"]


    human_test_pull_commits["pending-github-api-fetch"] = None


    # pra cada filename, insere um filename respectivo de teste, com cada test naming pattern, 
    # pra depois poder fazer um join com os commits humanos que tenham filenames de teste
    for index, row in bot_pull_request_commits.iterrows():
        filename_without_path = row["filename"].split('/')[-1]
        extension = filename_without_path.split('.')[-1]
        bot_pull_request_commits.at[index, "*filename*.test"] = filename_without_path.split('.' + extension)[0] + '.test' + '.' + extension
        bot_pull_request_commits.at[index, "*filename*.spec"] = filename_without_path.split('.' + extension)[0] + '.spec' + '.' + extension
        bot_pull_request_commits.at[index, "*filename*_spec"] = filename_without_path.split('.' + extension)[0] + '_spec' + '.' + extension
        bot_pull_request_commits.at[index, "*filename*_test"] = filename_without_path.split('.' + extension)[0] + '_test.' + extension
        bot_pull_request_commits.at[index, "*filename*_tests"] = filename_without_path.split('.' + extension)[0] + '_tests.' + extension
        bot_pull_request_commits.at[index, "*filename*Test"] = filename_without_path.split('.' + extension)[0] + 'Test.' + extension
        bot_pull_request_commits.at[index, "*filename*Tests"] = filename_without_path.split('.' + extension)[0] + 'Tests.' + extension
        bot_pull_request_commits.at[index, "Test*filename*"] = 'Test' + filename_without_path
        bot_pull_request_commits.at[index, "test_*filename*"] = 'test_' + filename_without_path

        if (
            (not row["filename"].startswith('test')) and
            (not row["filename"].startswith('__test')) and 
            (not '/test/' in row["filename"]) and 
            (not '/tests/' in row["filename"]) and
            (not '/__tests__/' in row["filename"])
            ) :
            filename_without_path = row["filename"].split('/')[-1]
            bot_pull_request_commits.at[index, "filename-test-folder"] = filename_without_path  



    test_root_dir_human_commits = human_test_pull_commits[
            ((human_test_pull_commits["filename"].str.startswith('test')) |
            (human_test_pull_commits["filename"].str.startswith('__test')
            ) &
            (human_test_pull_commits["filename"].str.contains('/'))
            ) |
            (human_test_pull_commits["filename"].str.contains('/test/')) |
            (human_test_pull_commits["filename"].str.contains('/tests/')) |
            (human_test_pull_commits["filename"].str.contains('/__tests__/'))
    ].copy()


    # arquivos de teste e arquivos testados podem estar em um path diferente (ex pasta /tests),
    # entao só comparamos o nome do arquivo, e nao o caminho completo
    human_test_pull_commits["filename"] = human_test_pull_commits["filename"].str.rsplit('/').str[-1]
    test_root_dir_human_commits["filename"] = test_root_dir_human_commits["filename"].str.rsplit('/').str[-1]



    dotSpecFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["*filename*.spec", "repo_url"], how='inner')
    dotSpecFiles["test-filename-pattern"] = '*filename*.spec'

    underscoreSpecFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["*filename*_spec", "repo_url"], how='inner')
    underscoreSpecFiles["test-filename-pattern"] = '*filename*_spec'

    dotTestFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["*filename*.test", "repo_url"], how='inner')
    dotTestFiles["test-filename-pattern"] = '*filename*.test'

    underscoreTestFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["*filename*_test", "repo_url"], how='inner')
    underscoreTestFiles["test-filename-pattern"] = '*filename*_test'

    underscoreTestsPluralFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["*filename*_tests", "repo_url"], how='inner')
    underscoreTestsPluralFiles["test-filename-pattern"] = '*filename*_tests'

    endswithTestFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["*filename*Test", "repo_url"], how='inner')
    endswithTestFiles["test-filename-pattern"] = '*filename*Test'

    endswithTestsPluralFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["*filename*Tests", "repo_url"], how='inner')
    endswithTestsPluralFiles["test-filename-pattern"] = '*filename*Tests'

    startswithTestFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["Test*filename*", "repo_url"], how='inner')
    startswithTestFiles["test-filename-pattern"] = 'Test*filename*'

    startswithUnderscoreTestFiles = pd.merge(human_test_pull_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["test_*filename*", "repo_url"], how='inner')
    startswithUnderscoreTestFiles["test-filename-pattern"] = 'test_*filename*'

    testFolders = pd.merge(test_root_dir_human_commits, bot_pull_request_commits, left_on=["filename", "repo_url"], right_on=["filename-test-folder", "repo_url"], how='inner')
    testFolders["test-filename-pattern"] = 'filename-test-folder'


    return pd.concat([dotSpecFiles, underscoreSpecFiles, dotTestFiles, underscoreTestFiles, underscoreTestsPluralFiles, endswithTestFiles, endswithTestsPluralFiles, startswithTestFiles, testFolders, startswithUnderscoreTestFiles])



def populate_commit_creation_date(final_dataframe_with_users):
    GITHUB_CONFIG = {
    'URL': 'https://api.github.com/repos/',
    'HEADERS': {
        ' Accept': 'Application/vnd-github+json*',
        'Authorization': ''
        },
    }


    for index, row in final_dataframe_with_users.iterrows():
        if row["commit_created_at_x"] is None or (not isinstance(row["commit_created_at_x"], str) and math.isnan(float(row["commit_created_at_x"]))):
            owner = None
            repoName = None
            if row["html_url_x"] is not None:
                temp = row["html_url_x"].split('/')
                owner = temp[3]
                repoName = temp[4]
                url = GITHUB_CONFIG[ 'URL'] + owner + '/' + repoName + '/commits/' + row["sha_x"]

                response = requests.get(url, GITHUB_CONFIG[ "HEADERS"], auth=('*github_user*','*github_token*'))

                if response.status_code == 200:
                    responseData = response.json()
                    final_dataframe_with_users.at[index, "commit_created_at_x"] = responseData["commit"]["committer"]["date"]
                else:
                    print(response.json())
                    print(row["author_x"])
                    print(url)
                    final_dataframe_with_users.at[index, "pending-github-api-fetch"] = row["author_x"]
                    
        
        if row["commit_created_at_y"] is None or (not isinstance(row["commit_created_at_y"], str) and math.isnan(float(row["commit_created_at_y"]))):
            owner = None
            repoName = None
            if row["html_url_y"] is not None:
                temp = row["html_url_y"].split('/')
                owner = temp[3]
                repoName = temp[4]
                url = GITHUB_CONFIG[ 'URL'] + owner + '/' + repoName + '/commits/' + row["sha_y"]

                response = requests.get(url, GITHUB_CONFIG[ "HEADERS"], auth=('*github_user*','*github_token*'))

                if response.status_code == 200:
                    responseData = response.json()
                    final_dataframe_with_users.at[index, "commit_created_at_y"] = responseData["commit"]["committer"]["date"]
                else:
                    print(response.json())
                    print(row["author_x"])
                    print(url)
                    final_dataframe_with_users.at[index, "pending-github-api-fetch"] = row["author_x"]

    return final_dataframe_with_users



def populate_dataframe_with_pr_reviews(filtered_results, all_commit_details_humans):
    pr_reviews = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_reviews.parquet")

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


    # seta default de 0 para usuarios que nao tiveram nenhuma interaçao em PR
    filtered_results_with_pr_info['count'] = filtered_results_with_pr_info['count'].apply(lambda count: 0 if math.isnan(count) else count)
    all_commits_with_pr_info['count'] = all_commits_with_pr_info['count'].apply(lambda count: 0 if math.isnan(count) else count)



    filtered_results_with_pr_info["count"] = filtered_results_with_pr_info["count"].astype(int)
    all_commits_with_pr_info["count"] = all_commits_with_pr_info["count"].astype(int)
    filtered_results_with_pr_info = filtered_results_with_pr_info.sort_values(by='count')
    all_commits_with_pr_info = all_commits_with_pr_info.sort_values(by='count')
    all_commits_with_pr_info = all_commits_with_pr_info[["sha", "author", "filename", "status", "count"]]

    filtered_results_with_pr_info = filtered_results_with_pr_info.rename(columns={'count':'pr_review_count'})
    all_commits_with_pr_info = all_commits_with_pr_info.rename(columns={'count':'pr_review_count'})
    return { 
        "filtered_results_with_pr_info": filtered_results_with_pr_info, 
        "all_commits_with_pr_info": all_commits_with_pr_info 
        }

