CREATE TABLE IF NOT EXISTS countries (
    id integer PRIMARY KEY,
    country_name text NOT NULL,
    country_code text NOT NULL,
    state_name text,
    state_code text,
    county_name text,
    county_code text,
    UNIQUE(country_name, country_code, state_name, state_code, county_name, county_code)
);

CREATE TABLE IF NOT EXISTS bcrcodes (id integer PRIMARY KEY, bcr_code text, UNIQUE(bcr_code));

CREATE TABLE IF NOT EXISTS ibacodes (id integer PRIMARY KEY, iba_code text, UNIQUE(iba_code));

CREATE TABLE IF NOT EXISTS species (
    id integer PRIMARY KEY,
    taxonomic_order integer,
    category text,
    common_name text,
    scientific_name text NOT NULL,
    subspecies_common_name text,
    subspecies_scientific_name text,
    UNIQUE(taxonomic_order, common_name, scientific_name, subspecies_common_name, subspecies_scientific_name)
);

CREATE TABLE IF NOT EXISTS observers (
    id integer PRIMARY KEY,
    string_id text NOT NULL,
    UNIQUE(string_id)
);

CREATE TABLE IF NOT EXISTS breeding (
    id integer PRIMARY KEY,
    breeding_code text,
    breeding_category text,
    behavior_code text,
    UNIQUE(breeding_code, breeding_category, behavior_code)
);

CREATE TABLE IF NOT EXISTS localities (
    id integer PRIMARY KEY,
    locality_name text,
    locality_code text,
    locality_type text,
    UNIQUE(locality_name)
);

CREATE TABLE IF NOT EXISTS protocols (
    id integer PRIMARY KEY,
    protocol_type text,
    protocol_code text,
    project_code text,
    UNIQUE(protocol_type, protocol_code, project_code)
);

CREATE TABLE IF NOT EXISTS observations (
    country_id integer,
    bcr_code_id integer,
    iba_code_id integer,
    species_id integer NOT NULL,
    observer_id integer,
    breeding_id integer,
    locality_id integer,
    protocol_id integer,
    global_unique_identifier text,
    last_edited_date text,
    observation_count integer,
    age_sex text,
    usfws_code text,
    atlas_block text,
    latitude float,
    longitude float,
    observation_date text,
    observations_started text,
    sampling_event_identifier text,
    duration_minutes integer,
    effort_distance float,
    effort_area float,
    number_observers integer,
    all_species_reported integer,
    group_identifier text,
    has_media integer,
    approved integer,
    reviewed integer,
    reason text,
    trip_comments text,
    species_comments text,
    FOREIGN KEY (species_id) REFERENCES species(id),
    FOREIGN KEY (bcr_code_id) REFERENCES bcrcodes(id),
    FOREIGN KEY (iba_code_id) REFERENCES ibacodes(id),
    FOREIGN KEY (breeding_id) REFERENCES breeding(id),
    FOREIGN KEY (country_id) REFERENCES countries(id),
    FOREIGN KEY (locality_id) REFERENCES localities(id),
    FOREIGN KEY (observer_id) REFERENCES observers(id)
    FOREIGN KEY (protocol_id) REFERENCES protocols(id)
);