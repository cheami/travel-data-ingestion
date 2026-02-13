DROP TABLE IF EXISTS silver.daily_spend;

CREATE TABLE silver.daily_spend (
    date DATE,
    type VARCHAR(255),
    load_id INTEGER,
    amount NUMERIC
);