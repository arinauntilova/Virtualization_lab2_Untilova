DROP TABLE IF EXISTS House_prices;

CREATE TABLE House_prices (
    id int primary key,
    property_type varchar(50),
    price numeric,
    location varchar(100),
    city varchar(100),
    baths int,
    purpose varchar(256),
    bedrooms int,
    Area_in_Marla numeric
);

TRUNCATE TABLE House_prices 
RESTART IDENTITY;

COPY House_prices(id, property_type, price, location, city, baths, purpose, bedrooms, Area_in_Marla)
FROM '/house_prices.csv' 
DELIMITER ',' 
CSV HEADER;