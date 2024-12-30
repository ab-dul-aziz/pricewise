CREATE TEMP TABLE house_prediction_table_temp (
    url TEXT,
    title TEXT,
    description TEXT,
    price_mio NUMERIC,
    address TEXT,
    city TEXT,
    land_size_m2 NUMERIC,
    building_size_m2 NUMERIC,
    bedroom NUMERIC,
    bathroom NUMERIC,
    garage NUMERIC,
    carport NUMERIC,
    property_type TEXT,
    certificate TEXT,
    voltage_watt NUMERIC,
    maid_bedroom NUMERIC,
    maid_bathroom NUMERIC,
    kitchen NUMERIC,
    dining_room NUMERIC,
    living_room NUMERIC,
    furniture TEXT,
    building_material TEXT,
    floor_material TEXT,
    floor_level NUMERIC,
    house_facing TEXT,
    concept_and_style TEXT,
    view TEXT,
    internet_access TEXT,
    road_width NUMERIC,
    year_built NUMERIC,
    year_renovated NUMERIC,
    water_source TEXT,
    corner_property BOOLEAN,
    property_condition TEXT,
    ad_type TEXT,
    ad_id TEXT
);

-- Perintah COPY tanpa ON CONFLICT
COPY house_prediction_table_temp (
    url, title, description, price_mio, address, city, land_size_m2, building_size_m2, bedroom, bathroom, garage,
    carport, property_type, certificate, voltage_watt, maid_bedroom, maid_bathroom, kitchen, dining_room, living_room,
    furniture, building_material, floor_material, floor_level, house_facing, concept_and_style, view, internet_access,
    road_width, year_built, year_renovated, water_source, corner_property, property_condition, ad_type, ad_id
)
FROM '/opt/airflow/data/data_cleaned.csv'
DELIMITER ',' CSV HEADER;

-- Tangani duplikasi di tabel utama
INSERT INTO house_prediction_table (
    url, title, description, price_mio, address, city, land_size_m2, building_size_m2, bedroom, bathroom, garage,
    carport, property_type, certificate, voltage_watt, maid_bedroom, maid_bathroom, kitchen, dining_room, living_room,
    furniture, building_material, floor_material, floor_level, house_facing, concept_and_style, view, internet_access,
    road_width, year_built, year_renovated, water_source, corner_property, property_condition, ad_type, ad_id
)
SELECT
    t.url, t.title, t.description, t.price_mio, t.address, t.city, t.land_size_m2, t.building_size_m2, t.bedroom, t.bathroom, t.garage,
    t.carport, t.property_type, t.certificate, t.voltage_watt, t.maid_bedroom, t.maid_bathroom, t.kitchen, t.dining_room, t.living_room,
    t.furniture, t.building_material, t.floor_material, t.floor_level, t.house_facing, t.concept_and_style, t.view, t.internet_access,
    t.road_width, t.year_built, t.year_renovated, t.water_source, t.corner_property, t.property_condition, t.ad_type, t.ad_id
FROM house_prediction_table_temp t
ON CONFLICT (url) DO UPDATE
SET
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    price_mio = EXCLUDED.price_mio,
    address = EXCLUDED.address,
    city = EXCLUDED.city,
    land_size_m2 = EXCLUDED.land_size_m2,
    building_size_m2 = EXCLUDED.building_size_m2,
    bedroom = EXCLUDED.bedroom,
    bathroom = EXCLUDED.bathroom,
    garage = EXCLUDED.garage,
    carport = EXCLUDED.carport,
    property_type = EXCLUDED.property_type,
    certificate = EXCLUDED.certificate,
    voltage_watt = EXCLUDED.voltage_watt,
    maid_bedroom = EXCLUDED.maid_bedroom,
    maid_bathroom = EXCLUDED.maid_bathroom,
    kitchen = EXCLUDED.kitchen,
    dining_room = EXCLUDED.dining_room,
    living_room = EXCLUDED.living_room,
    furniture = EXCLUDED.furniture,
    building_material = EXCLUDED.building_material,
    floor_material = EXCLUDED.floor_material,
    floor_level = EXCLUDED.floor_level,
    house_facing = EXCLUDED.house_facing,
    concept_and_style = EXCLUDED.concept_and_style,
    view = EXCLUDED.view,
    internet_access = EXCLUDED.internet_access,
    road_width = EXCLUDED.road_width,
    year_built = EXCLUDED.year_built,
    year_renovated = EXCLUDED.year_renovated,
    water_source = EXCLUDED.water_source,
    corner_property = EXCLUDED.corner_property,
    property_condition = EXCLUDED.property_condition,
    ad_type = EXCLUDED.ad_type,
    ad_id = EXCLUDED.ad_id;

DROP TABLE IF EXISTS house_prediction_table_temp;
