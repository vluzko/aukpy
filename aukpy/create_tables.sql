CREATE TABLE IF NOT EXISTS location_data (
    id integer PRIMARY KEY,
    country_name text NOT NULL,
    country_code text NOT NULL,
    state_name text,
    state_code text,
    county_name text,
    county_code text,
    locality,
    locality_id,
    locality_type,
    UNIQUE(country_name, country_code, state_name, state_code, county_name, county_code, locality, locality_id, locality_type)
);

CREATE TABLE IF NOT EXISTS bcrcode (id integer PRIMARY KEY, bcr_code text, UNIQUE(bcr_code));

CREATE TABLE IF NOT EXISTS ibacode (id integer PRIMARY KEY, iba_code text, UNIQUE(iba_code));

CREATE TABLE IF NOT EXISTS species (
    id integer PRIMARY KEY,
    taxonomic_order integer,
    category text,
    common_name text,
    scientific_name text NOT NULL,
    subspecies_common_name text,
    subspecies_scientific_name text,
    taxon_concept_id text,
    UNIQUE(taxonomic_order, category, common_name, scientific_name, subspecies_common_name, subspecies_scientific_name, taxon_concept_id)
);

-- CREATE TABLE IF NOT EXISTS observer (
--     id integer PRIMARY KEY,
--     string_id text NOT NULL,
--     UNIQUE(string_id)
-- );

CREATE TABLE IF NOT EXISTS breeding (
    id integer PRIMARY KEY,
    breeding_code text,
    breeding_category text,
    behavior_code text,
    UNIQUE(breeding_code, breeding_category, behavior_code)
);

-- Deprecated for now
-- CREATE TABLE IF NOT EXISTS localities (
--     id integer PRIMARY KEY,
--     locality_name text,
--     locality_code text,
--     locality_type text,
--     UNIQUE(locality_name)
-- );

CREATE TABLE IF NOT EXISTS protocol (
    id integer PRIMARY KEY,
    protocol_type text,
    protocol_code text,
    project_code text,
    UNIQUE(protocol_type, protocol_code, project_code)
);

CREATE TABLE IF NOT EXISTS observation (
    id integer PRIMARY KEY,
    location_data_id integer,
    bcrcode_id integer,
    ibacode_id integer,
    species_id integer NOT NULL,
    observer_id integer NOT NULL,
    breeding_id integer,
    protocol_id integer,
    global_unique_identifier text NOT NULL,
    last_edited_date text,
    observation_count integer,
    age_sex text,
    usfws_code text,
    atlas_block text,
    latitude float,
    longitude float,
    observation_date text,
    time_observations_started text,
    sampling_event_identifier text,
    duration_minutes integer,
    effort_distance_km float,
    effort_area_ha float,
    number_observers integer,
    all_species_reported integer,
    group_identifier text,
    has_media integer,
    approved integer,
    reviewed integer,
    reason text,
    trip_comments text,
    species_comments text,
    exotic_code text,
    FOREIGN KEY (species_id) REFERENCES species(id),
    FOREIGN KEY (bcrcode_id) REFERENCES bcrcode(id),
    FOREIGN KEY (ibacode_id) REFERENCES ibacode(id),
    FOREIGN KEY (breeding_id) REFERENCES breeding(id),
    FOREIGN KEY (location_data_id) REFERENCES location_data(id),
    FOREIGN KEY (protocol_id) REFERENCES protocol(id)
);