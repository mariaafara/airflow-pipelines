-- Create the "users" table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

-- Create the "transformed_users" table
CREATE TABLE transformed_users (
    id SERIAL PRIMARY KEY,
    name_uppercase VARCHAR(100),
    email_domain VARCHAR(100)
);
