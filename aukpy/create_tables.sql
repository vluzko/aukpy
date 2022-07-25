CREATE TABLE IF NOT EXISTS location_data (
    id integer PRIMARY KEY,
    country text NOT NULL,
    country_code text NOT NULL,
    state text,
    state_code text,
    county text,
    county_code text,
    locality text,
    locality_id integer,
    locality_type text,
    usfws_code text,
    atlas_block text,
    bcr_code float,
    iba_code text,
    UNIQUE(country, country_code, state, state_code, county, county_code, locality, locality_id, locality_type, usfws_code, atlas_block)
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

CREATE TABLE IF NOT EXISTS breeding (
    id integer PRIMARY KEY,
    breeding_code text,
    breeding_category text,
    behavior_code text,
    UNIQUE(breeding_code, breeding_category, behavior_code)
);

CREATE TABLE IF NOT EXISTS protocol (
    id integer PRIMARY KEY,
    protocol_type text,
    protocol_code text,
    project_code text,
    UNIQUE(protocol_type, protocol_code, project_code)
);

CREATE TABLE IF NOT EXISTS observer (
    id integer PRIMARY KEY,
    observer_id text NOT NULL
);

CREATE TABLE IF NOT EXISTS sampling_event (
    id integer PRIMARY KEY,
    sampling_event_identifier integer,
    observer_id integer NOT NULL,
    observation_date text,
    time_observations_started text,
    effort_distance_km float,
    effort_area_ha float,
    duration_minutes integer,
    trip_comments text,
    latitude float,
    longitude float,
    all_species_reported integer,
    number_observers integer,
    UNIQUE(sampling_event_identifier)
);

CREATE TABLE IF NOT EXISTS observation (
    id integer PRIMARY KEY,
    location_data_id integer NOT NULL,
    -- bcrcode_id integer,
    -- ibacode_id integer,
    species_id integer NOT NULL,
    breeding_id integer,
    protocol_id integer,
    sampling_event_id integer NOT NULL,
    global_unique_identifier int NOT NULL,
    last_edited_date text,
    observation_count integer,
    age_sex text,
    group_identifier text,
    has_media integer,
    approved integer,
    reviewed integer,
    reason text,
    species_comments text,
    exotic_code text,
    FOREIGN KEY (sampling_event_id) REFERENCES sampling_event(id),
    FOREIGN KEY (species_id) REFERENCES species(id),
    -- FOREIGN KEY (bcrcode_id) REFERENCES bcrcode(id),
    -- FOREIGN KEY (ibacode_id) REFERENCES ibacode(id),
    FOREIGN KEY (breeding_id) REFERENCES breeding(id),
    FOREIGN KEY (location_data_id) REFERENCES location_data(id),
    FOREIGN KEY (protocol_id) REFERENCES protocol(id)
);