-- pc1 busca commits que tenham arquivos de teste e que nao sejam feitos por bots
-- na mesma tabela, pc2 busca commits com filenames referentes aos arquivos testados e que sejam feitos por bots
-- verifica que pc1 tenha sido criado em uma PR posterior ao fechamento da PR de pc2 OU que façam parte da mesma PR
-- WITH r_test AS (
  SELECT DISTINCT (pc1.sha), 
  -- pc2.sha as bot_commit_sha, 
  -- pc1.pr_id, pc1.patch, 
  pc1.committer
  -- pc2.committer as bot_commiter, 
  -- pr1.html_url as human_html_url, 
  -- pr1.repo_url, 
  -- pr2.html_url as bot_html_url
  FROM pr_commit_details pc1, pr_commit_details pc2
  INNER JOIN pull_request pr1
  ON pc1.pr_id = pr1.id
  INNER JOIN pull_request pr2
  ON pc2.pr_id = pr2.id
  -- INNER JOIN user
  -- ON pc1.committer = user.login -- timeout, falta um index no schema
  WHERE
    pc1.sha != pc2.sha
    AND pr1.repo_url == pr2.repo_url
    AND pr2.merged_at IS NOT NULL
    AND (
      pr1.created_at > pr2.merged_at
      OR pc1.pr_id == pc2.pr_id --falta um campo de commit timestamp ou pc1.id numerico no schema
      )
    AND 
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
-- )

