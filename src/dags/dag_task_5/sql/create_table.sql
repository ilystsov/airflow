CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    location VARCHAR(50),
    date DATE,
    avg_temp_c FLOAT,
    condition VARCHAR(100)
    );
