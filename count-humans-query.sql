SELECT COUNT(DISTINCT (pc1.committer))
  FROM pr_commit_details pc1, pr_commit_details pc2
  INNER JOIN pull_request pr1
  ON pc1.pr_id = pr1.id
  INNER JOIN pull_request pr2
  ON pc2.pr_id = pr2.id
  WHERE
    (
      pc1.filename LIKE '%test.ts'
      OR
      pc1.filename LIKE '%test.tsx'
      OR
      pc1.filename LIKE '%spec.ts'
      OR
      pc1.filename LIKE '%spec.tsx'
    )
    AND regexp_matches(pc1.patch, '\n\+\s*((describe|it|test)(\.(only|skip))?|fdescribe|fit|xdescribe|xit)\s*\(')
    AND (
    pc1.committer NOT LIKE '%[bot]'
    AND
    pc1.committer != 'cursoragent'
    AND
    pc1.committer != 'Claude Bot'
    AND
    pc1.committer != 'Copilot'
    )
    AND pc1.sha != pc2.sha
    AND pr1.repo_url == pr2.repo_url
    AND pr2.merged_at IS NOT NULL
    AND (
      pr1.created_at > pr2.merged_at
      OR pc1.pr_id == pc2.pr_id --falta um campo de commit timestamp ou pc1.id numerico no schema
      )
    AND (
      pc2.committer LIKE '%[bot]'
      OR
      pc2.committer == 'cursoragent'
      OR
      pc2.committer == 'Claude Bot'
      OR
      pc2.committer == 'Copilot'
    )
    AND (
        pc2.filename == replace(pc1.filename, 'tsx', 'test.tsx')
        OR
        pc2.filename == replace(pc1.filename, 'ts', 'test.ts')
        OR
        pc2.filename == replace(pc1.filename, 'tsx', 'spec.tsx')
        OR
        pc2.filename == replace(pc1.filename, 'ts', 'spec.ts')
    )
--result: 14