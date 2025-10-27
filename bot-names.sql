SELECT distinct(user) FROM pr_comments WHERE user_type != 'User';
SELECT distinct(user) FROM pr_comments WHERE user_type != 'User' AND user NOT LIKE '%[bot]';