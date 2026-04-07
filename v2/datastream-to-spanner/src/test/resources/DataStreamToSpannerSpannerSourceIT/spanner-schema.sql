CREATE TABLE Users (
  UserId     INT64 NOT NULL,
  FirstName  STRING(100),
  LastName   STRING(100),
  Email      STRING(255),
  Active     BOOL,
  CreatedAt  TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (UserId);
