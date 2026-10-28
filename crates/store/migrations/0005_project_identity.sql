CREATE UNIQUE INDEX projects_git_common_dir_identity
ON projects(git_common_dir_identity)
WHERE git_common_dir_identity IS NOT NULL;
