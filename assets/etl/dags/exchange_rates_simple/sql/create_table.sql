CREATE SCHEMA IF NOT EXISTS seminar_11;
CREATE TABLE IF NOT EXISTS seminar_11.currency_exchange_rates_simple
(
    base     VARCHAR(3)     NOT NULL,
    currency VARCHAR(3)     NOT NULL,
    rate     NUMERIC(12, 3) NOT NULL,
    date     DATE           NOT NULL,
    UNIQUE (base, currency, date)
);
