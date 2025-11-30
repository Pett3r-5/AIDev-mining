import pandas as pd
from data_mappers import build_human_and_bot_commit_data, map_test_files_to_tested_files, populate_commit_creation_date, populate_dataframe_with_pr_reviews
from statistics import calculate_stats



def main():
    builtData = build_human_and_bot_commit_data()
    matched_test_commits = map_test_files_to_tested_files(builtData["bot_pull_request_commits"], builtData["human_test_pull_commits"])

    print('null merge dates')
    print(len(matched_test_commits[
        (matched_test_commits["pr_id_x_x"] != matched_test_commits["pr_id_x_y"]) &
        matched_test_commits["merged_at_y"].isnull()]))

    matched_test_commits = matched_test_commits[
        (   
            (matched_test_commits["pr_id_x_x"] != matched_test_commits["pr_id_x_y"]) &
            (
                (
                matched_test_commits["merged_at_y"].notnull() &
                (matched_test_commits["created_at_x"] > matched_test_commits["merged_at_y"])
                ) |
                (
                matched_test_commits["merged_at_y"].isnull() &
                (matched_test_commits["created_at_x"] > matched_test_commits["created_at_y"])
                )
            )
            
        )
        |  (matched_test_commits["pr_id_x_x"] == matched_test_commits["pr_id_x_y"])
                    ]
    
    
    same_pr_commits = matched_test_commits[(matched_test_commits["pr_id_x_x"] == matched_test_commits["pr_id_x_y"])]

    pr_commits_with_dates = populate_commit_creation_date(same_pr_commits)

    same_pr_commits_matched = pr_commits_with_dates[
                pr_commits_with_dates["commit_created_at_x"].isnull() |
                pr_commits_with_dates["commit_created_at_y"].isnull() |
                (pr_commits_with_dates["commit_created_at_x"] > pr_commits_with_dates["commit_created_at_y"])
    ]

    
    final_dataframe_with_matching_dates = pd.concat([matched_test_commits, same_pr_commits_matched])

    final_dataframe_with_matching_dates = final_dataframe_with_matching_dates.drop_duplicates(subset=['author_x'])

    all_users = builtData["all_users"].rename(columns={'created_at':'user_created_at'})
    final_dataframe_with_users = pd.merge(final_dataframe_with_matching_dates, all_users, left_on="author_x", right_on="login", how='left').sort_values(by='user_created_at')

    final_dataframe_with_users = final_dataframe_with_users.rename(columns={'author_x':'human_username'})
    final_dataframe_with_users = final_dataframe_with_users.rename(columns={'author_y':'agent_username'})
    final_dataframe_with_users = final_dataframe_with_users.rename(columns={'html_url_x':'human_html_url'})
    final_dataframe_with_users = final_dataframe_with_users.rename(columns={'html_url_y':'agent_html_url'})
    final_dataframe_with_users = final_dataframe_with_users.rename(columns={'sha_x':'human_sha'})
    final_dataframe_with_users = final_dataframe_with_users.rename(columns={'sha_y':'agent_sha'})



    filtered_results = final_dataframe_with_users[["human_html_url", "agent_html_url", "human_sha", "agent_sha", "human_username", "agent_username", "original-filename", "user_created_at", "test-filename-pattern", "pending-github-api-fetch"]]


    result = populate_dataframe_with_pr_reviews(filtered_results, builtData["general_population_final"])

    result["filtered_results_with_pr_info"].to_csv("selected-users.csv")
    result["all_commits_with_pr_info"].to_csv("all-users.csv")


    calculate_stats(result["filtered_results_with_pr_info"], result["all_commits_with_pr_info"])




main()