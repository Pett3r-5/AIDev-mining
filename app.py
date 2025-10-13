import pandas as pd
import re

all_comments = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_comments.parquet")
human_comments = all_comments[all_comments["user_type"] != "Bot"]

test_comments = human_comments[human_comments["body"].str.contains("test", re.IGNORECASE)]

unique_users = {}

for test_comm_user_id in test_comments["user_id"]:
    if test_comm_user_id in unique_users:
        unique_users[test_comm_user_id]["comments_count"] += 1
    else:
        unique_users[test_comm_user_id] = {}
        unique_users[test_comm_user_id]["comments_count"] = 1


print("Unique users")
print(unique_users)
print("\n")


users_detail = pd.read_parquet("hf://datasets/hao-li/AIDev/user.parquet")
print("users created_at")
print(users_detail["created_at"])

# pra cada unique_users key, buscar a sua users_detail.created_at