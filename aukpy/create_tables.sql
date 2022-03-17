CREATE TABLE IF NOT EXISTS countries (
    id: integer PRIMARY KEY,
    country_name: text NOT NULL,
    country_code: text NOT NULL,
    state_name: text,
    state_code: text,
    county_name: text,
    county_code: text
    UNIQUE(country_name, country_code, state_name, state_code, county_name, county_code)
);

CREATE TABLE IF NOT EXISTS bcrcodes (code: integer PRIMARY KEY, bcr_name: text, UNIQUE(bcr_name));

CREATE TABLE IF NOT EXISTS ibacodes (code: integer PRIMARY KEY, iba_name: text, UNIQUE(iba_name));

CREATE TABLE IF NOT EXISTS species (
    id: integer PRIMARY KEY,
    common_name: text,
    scientific_name: text NOT NULL,
    subspecies_common_name: text,
    subspecies_scientific_name: text,
    UNIQUE(common_name, scientific_name, subspecies_common_name, subspecies_scientific_name)
);

CREATE TABLE IF NOT EXISTS observers (
    id: integer PRIMARY KEY,
    string_id: text NOT NULL,
    UNIQUE(string_id)
);

CREATE TABLE IF NOT EXISTS categories (
    id: integer PRIMARY KEY,
    category_name: text NOT NULL,
    UNIQUE(category_name)
)

CREATE TABLE IF NOT EXISTS breeding_codes (
    id: integer PRIMARY KEY,
    breeding_category: text,
    UNIQUE(breeding_category)
)

CREATE TABLE IF NOT EXISTS breeding_codes (
    id: integer PRIMARY KEY,
    breeding_code: text,
    UNIQUE(breeding_code)
)

-- Not sure if this can be combined with previous
CREATE TABLE IF NOT EXISTS breeding_categories (
    id: integer PRIMARY KEY,
    breeding_category: text,
    UNIQUE(breeding_category)
)

CREATE TABLE IF NOT EXISTS behavior_codes (
    id: integer PRIMARY KEY,
    behavior_code: text,
    UNIQUE(behavior_code)
)

CREATE TABLE IF NOT EXISTS localities (
    id: integer PRIMARY KEY,
    locality_name: text,
    locality_code: text,
    locality_type: text,
    UNIQUE(locality_name)
)

CREATE TABLE IF NOT EXISTS observations (
    global_unique_identifier: text,
    last_edited_date: text,
    species_id: integer,
    category_id: integer,
    observation_count: integer,
    breeding_code_id: integer,
    breeding_category_id: integer,
    behavior_code_id: integer,
    age_sex: text,
    country_id: integer,
    iba_code_id: integer,
    bcr_code_id: integer,
    FOREIGN KEY (species_id) REFERENCES species(id),
    FOREIGN KEY (category_id) REFERENCES categories(id),
    FOREIGN KEY (breeding_code_id) REFERENCES breeding_codes(id),
    FOREIGN KEY (breeding_category_id) REFERENCES breeding_categorys(id),
    FOREIGN KEY (behavior_code_id) REFERENCES behavior_code(id),
    FOREIGN KEY (country_id) REFERENCES countries(id)
);