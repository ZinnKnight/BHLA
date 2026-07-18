CREATE TABLE IF NOT EXISTS users (
                                     user_id       UUID PRIMARY KEY,
                                     user_name     TEXT NOT NULL UNIQUE,
                                     user_password TEXT NOT NULL,
                                     user_role     TEXT NOT NULL            
);

