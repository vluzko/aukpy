library(auk)
f_in <- "performance/observations.txt"
f_out <- "performance/filtered.txt"
ebird_data <- f_in %>%
# 1. reference file
auk_ebd() %>%
# 2. define filters
auk_species(species = "Black-capped Chickadee") %>%
auk_country(country = "United States") %>%
# 3. run filtering
auk_filter(file = f_out, overwrite = TRUE) %>%
# 4. read text file into r data frame
read_ebd()
