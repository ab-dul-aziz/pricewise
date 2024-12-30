-- Create database
CREATE DATABASE house_prediction_db;

-- You need to change the database connection to house_prediction_db before run the syntax below

BEGIN;
CREATE TABLE house_prediction_table (
	url TEXT,
    title TEXT,
    description TEXT,
    price_mio FLOAT,
    address TEXT,
    city TEXT,
    land_size_m2 FLOAT,
    building_size_m2 FLOAT,
    bedroom FLOAT,
    bathroom FLOAT,
    garage FLOAT,
    carport FLOAT,
    property_type TEXT,
    certificate TEXT,
    voltage_watt FLOAT,
    maid_bedroom FLOAT,
    maid_bathroom FLOAT,
    kitchen FLOAT,
    dining_room FLOAT,
    living_room FLOAT,
    furniture TEXT,
    building_material TEXT,
    floor_material TEXT,
    floor_level FLOAT,
    house_facing TEXT,
    concept_and_style TEXT,
    view TEXT,
    internet_access TEXT,
    road_width FLOAT,
    year_built FLOAT,
    year_renovated FLOAT,
    water_source TEXT,
    corner_property BOOLEAN,
    property_condition TEXT,
    ad_type TEXT,
    ad_id TEXT
);

COPY house_prediction_table (
    url, title, description, price_mio, address, city, land_size_m2, building_size_m2,
    bedroom, bathroom, garage, carport, property_type, certificate, voltage_watt,
    maid_bedroom, maid_bathroom, kitchen, dining_room, living_room, furniture,
    building_material, floor_material, floor_level, house_facing, concept_and_style,
    view, internet_access, road_width, year_built, year_renovated, water_source,
    corner_property, property_condition, ad_type, ad_id
)
FROM '/opt/airflow/data/data_cleaned.csv'
DELIMITER ','
CSV HEADER;

-- Run this code if you need to rollback
-- ROLLBACK;

COMMIT;