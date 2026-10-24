-- Throws away what is in project.head_run.
ALTER TABLE project DROP COLUMN head_run;

-- Throws away what is in project.head_version.
ALTER TABLE project DROP COLUMN head_version;
