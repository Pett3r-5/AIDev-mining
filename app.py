import pandas as pd

test_commits_for_bot_correction = pd.read_csv("test-code-commits-by-humans-and-bots.csv")

unique_users = {}
print(test_commits_for_bot_correction)

for username in test_commits_for_bot_correction["human_committer"]:
    if username in unique_users:
        unique_users[username]["commits_count"] += 1
    else:
        unique_users[username] = {}
        unique_users[username]["commits_count"] = 1


users_detail = pd.read_parquet("hf://datasets/hao-li/AIDev/user.parquet")

invalid_users = []
for key in unique_users:
    user_detail = users_detail[users_detail["login"] == key]
    
    if not user_detail["created_at"].empty:
        unique_users[key]["created_at"] = user_detail["created_at"].values[0]
    else:
        print("invalid user")
        print(key)
        print("\n")
        invalid_users.append(key)

for invalid_user in invalid_users:
    del unique_users[invalid_user]

print("unique_users: ", unique_users)
print("\n")
print("Length: ", len(unique_users))