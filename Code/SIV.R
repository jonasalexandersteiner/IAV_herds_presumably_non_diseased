# ===========================
# SIV Farms Data Cleaning Pipeline
# ===========================
# This script performs robust, transparent cleaning and transformation of farm-level data
# for SIV risk analysis, following best practices in reproducible data science.
#
# STRUCTURE:
# 1. Environment Setup
# 2. Library Management
# 3. Directory Preparation
# 4. Data Loading and Initial Cleaning
# 5. Helper Functions (with detailed logic & error handling)
# 6. Data Transformation Pipeline (with explicit type conversions and relocation)
# 7. Quick Data Overview
# ===========================

# ---- 1. Environment Setup ----
Sys.setenv(LANG = "en")     # Ensure English messages
options(scipen = 999)       # Avoid scientific notation for numerics
rm(list = ls())             # Remove all objects from workspace for a clean start

# ---- 2. Library Management ----
library(pacman)
# Load all required packages (will install if missing)
pacman::p_load(
  tidyverse, haven, readxl, writexl, janitor, lubridate, DT,
  broom, purrr, stringr, knitr, kableExtra ,deeplr
)

# ---- 3. Directory Preparation ----
d_proj <- "C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/IVA_Projekt/Data_processing/SIV_farms"
setwd(d_proj)
dirs <- file.path(d_proj, c("01_oridata", "02_data", "03_code", "04_output"))
# Create directories if they do not exist (recursive for nested folders)
walk(dirs, ~ if(!dir.exists(.x)) dir.create(.x, recursive = TRUE))
d_oridata <- dirs[1]

# ---- 4. Data Loading and Initial Cleaning ----
# Read main Excel file, treat "NA" as missing, clean column names to snake_case
sivfarms <- read_excel(file.path(d_oridata, "SIV_farms.xlsx"), na = "NA") %>%
  clean_names()
df1 <- sivfarms

# ---- 5. Helper Functions ----

# 5a. Feeding Site Category Extraction
#    - Extract, categorize feeding site information using pattern matching and numeric binning
make_category <- function(x) {
  if (is.na(x)) return("NA")
  # Pattern: "Tiere/Automat" with leading number
  if (grepl("Tiere/Automat", x, ignore.case = TRUE)) {
    num <- as.numeric(sub("^([0-9]+\\.?[0-9]*)\\s*Tiere/Automat.*$", "\\1", x))
    if (!is.na(num)) {
      if (num < 25) return("under 25 Animals/feeder")
      return("over 25 Animals/feeder")
    }
  }
  # Pattern: just a number (trough size)
  if (grepl("^\\s*-?\\d*\\.?\\d+\\s*$", x)) {
    num <- as.numeric(x)
    if (!is.na(num)) {
      if (num < 25) return("under 25cm trough/animal")
      return("over 25cm trough/animal")
    }
  }
  return("Other")
}
category_levels <- c(
  "under 25 Animals/feeder", "over 25 Animals/feeder",
  "under 25cm trough/animal", "over 25cm trough/animal",
  "Other", "NA"
)

# 5b. Age (weeks) Factor Extraction
#    - Convert age group text to categorical bins ("0-4w", "5-8w", ...)
age_weeks_factor <- function(x) {
  if (is.na(x) || !nzchar(trimws(x))) return(NA_character_)
  x <- trimws(x)
  nums <- as.numeric(str_extract_all(x, "\\d+")[[1]])
  if (length(nums) == 0) return(NA_character_)
  get_bin <- function(n) {
    cut(n, breaks = c(-Inf, 4, 8, 12, Inf),
        labels = c("0-4w", "5-8w", "9-12w", "13+w"),
        right = TRUE)
  }
  bins <- unique(as.character(get_bin(nums)))
  if (length(bins) == 1) {
    return(bins)
  } else {
    if (max(nums, na.rm = TRUE) <= 10) return("mixed_ages<10w")
    return("mixed_ages>10w")
  }
}

# 5c. Logical Conversion
#    - Map German, English, numeric and boolean values to TRUE/FALSE/NA
make_logical <- function(x) {
  case_when(
    is.na(x) ~ NA,
    x %in% c("WAHR", "Ja", "ja", "1", 1, TRUE, T, "TRUE", "true") ~ TRUE,
    x %in% c("FALSCH", "Nein", "nein", "0", 0, FALSE, F, "FALSE", "false") ~ FALSE,
    TRUE ~ NA
  )
}

# 5d. Safe pattern matching function (error-proof)
#    - Safely match patterns without regex errors
safe_match <- function(pattern, x) {
  if (is.na(x) || !is.character(x)) return(FALSE)
  tryCatch(grepl(pattern, x, fixed = TRUE), error = function(e) FALSE)
}

# ---- 6. Pre-processing for problematic frequency_respi_outbreak field ----
# Create safe temporary data frame for categorization
df_temp <- data.frame(
  original = df1$frequency_respiratory_disease_outbreaks,
  category = rep("once", nrow(df1))  # Default value
)

# Process each row individually with safe pattern matching
for (i in 1:nrow(df_temp)) {
  val <- df_temp$original[i]
  
  # NA or empty handling
  if (is.na(val) || trimws(as.character(val)) == "") {
    df_temp$category[i] <- "never"
    next
  }
  
  # Convert to character and lowercase
  val_char <- tolower(as.character(val))
  
  # Simple matching with fixed=TRUE to avoid regex issues
  if (safe_match("few pigs", val_char) || 
      safe_match("kein ausbruch", val_char) || 
      safe_match("keine klinik", val_char)) {
    df_temp$category[i] <- "few pigs affected no outbreak"
  } else if (safe_match("vereinzelt", val_char)) {
    df_temp$category[i] <- "few pigs affected no outbreak"
  } else if (safe_match("wiederkehrend", val_char) || 
             safe_match("regelmässig", val_char) || 
             safe_match("regelmaessig", val_char)) {
    df_temp$category[i] <- "3x or more per year"
  } else if (safe_match("1x/mal pro jahr", val_char) || 
             safe_match("1 mal pro jahr", val_char) || 
             safe_match("einmal pro jahr", val_char) || 
             safe_match("1mal pro jahr", val_char)) {
    df_temp$category[i] <- "1x/year"
  } else if (safe_match("2x/mal pro jahr", val_char) || 
             safe_match("2 mal pro jahr", val_char)) {
    df_temp$category[i] <- "2x/year"
  } else if (safe_match("3x/mal pro jahr", val_char) || 
             safe_match("3 mal pro jahr", val_char)) {
    df_temp$category[i] <- "3x or more per year"
  } else if (safe_match("einmal", val_char)) {
    df_temp$category[i] <- "once"
  } else if (val_char == "1x" || val_char == "1") {
    df_temp$category[i] <- "once"
  } else if (val_char == "2x" || val_char == "2") {
    df_temp$category[i] <- "2x/year"
  } else if (val_char == "3x" || val_char == "3") {
    df_temp$category[i] <- "3x or more per year"
  } else if (safe_match("pro winter", val_char) || 
             safe_match("pro jahr", val_char)) {
    # Extract number if possible
    num_match <- regexpr("\\d+", val_char)
    if (num_match > 0) {
      num_str <- substr(val_char, num_match, num_match + attr(num_match, "match.length") - 1)
      num <- as.numeric(num_str)
      if (!is.na(num)) {
        if (num == 1) df_temp$category[i] <- "1x/year"
        else if (num == 2) df_temp$category[i] <- "2x/year"
        else if (num >= 3) df_temp$category[i] <- "3x or more per year"
      }
    }
  }
}

# ---- 7. Data Transformation Pipeline ----
df2 <- df1 %>%
  # 7a. Factor conversions (explicit for categorical variables)
  mutate(
    canton = as.factor(canton),
    production_type = as.factor(production_type),
    sgd_qgs = as.factor(sgd_qgs),
    mode_stable_occupation_ai_centre = as.factor(mode_stable_occupation_ai_centre),
    mode_stable_occupation_gilts_stable = as.factor(mode_stable_occupation_gilts_stable),
    mode_stable_occupation_farrowing_stable = as.factor(mode_stable_occupation_farrowing_stable),
    litter_equalization_farrowing_stable = as.factor(litter_equalization_farrowing_stable),
    mode_stable_occupation_weaner_stable = as.factor(mode_stable_occupation_weaner_stable),
    mode_stable_occupation_fattener_stable = as.factor(mode_stable_occupation_fattener_stable),
    isolation_respiratory_dieseased_pigs = as.factor(isolation_respiratory_dieseased_pigs),
    cleaning_ai_centre = as.factor(cleaning_ai_centre),
    cleaning_gilts_stable = as.factor(cleaning_gilts_stable),
    cleaning_farrowing_stable = as.factor(cleaning_farrowing_stable),
    cleaning_weaner_stable = as.factor(cleaning_weaner_stable),
    cleaning_fattener_stable = as.factor(cleaning_fattener_stable),
    cleaning_quarantaine = as.factor(cleaning_quarantaine),
    desinfection_ai_centre = as.factor(desinfection_ai_centre),
    desinfection_gilts_stable = as.factor(desinfection_gilts_stable),
    desinfection_farrowing_stable = as.factor(desinfection_farrowing_stable),
    desinfection_weaner_stable = as.factor(desinfection_weaner_stable),
    desinfection_fattener_stable = as.factor(desinfection_fattener_stable),
    desinfection_quarantaine = as.factor(desinfection_quarantaine),
    drying_ai_centre = as.factor(drying_ai_centre),
    drying_gilts_stable = as.factor(drying_gilts_stable),
    drying_farrowing_stable = as.factor(drying_farrowing_stable),
    drying_weaner_stable = as.factor(drying_weaner_stable),
    drying_fattener_stable = as.factor(drying_fattener_stable),
    drying_quarantaine = as.factor(drying_quarantaine),
    cleaning_desinfection_transport_vehicle = as.factor(cleaning_desinfection_transport_vehicle),
    cleaning_shipment_area = as.factor(cleaning_shipment_area),
    caretaker_type = as.factor(caretaker_type),
    caretaker_ppe_stable = as.factor(caretaker_ppe_stable),
    caretaker_ppe_washing_interval = as.factor(caretaker_ppe_washing_interval),
    caretaker_ppe_per_unit = as.factor(caretaker_ppe_per_unit),
    caretaker_disease_management = as.factor(caretaker_disease_management),
    visitors_disease_management = as.factor(visitors_disease_management),
    visitors_contact_other_pigs = as.factor(visitors_contact_other_pigs),
    visitors_contact_poultry = as.factor(visitors_contact_poultry),
    return_to_service_rate = as.factor(return_to_service_rate),
    farrowing_rate = as.factor(farrowing_rate),
    piglets_per_sow_year = as.factor(piglets_per_sow_year),
    abortions_per_sow_year = as.factor(abortions_per_sow_year),
    piglet_mortality = as.factor(piglet_mortality),
    growing_rate_weaners = as.factor(growing_rate_weaners),
    growing_rate_fatteners = as.factor(growing_rate_fatteners),
    feed_conversion_rate_weaners = as.factor(feed_conversion_rate_weaners),
    feed_conversion_rate_fatteners = as.factor(feed_conversion_rate_fatteners),
    respiratory_history_swine = as.factor(respiratory_history_swine),
    time_respiratory_disease = as.factor(time_respiratory_disease),
    visitors_in_stable = as.factor(visitors_in_stable),
    symptom_severity = as.factor(symptom_severity),
    visitors_cumulative_contact_hours = as.factor(visitors_cumulative_contact_hours),
    ppe_visitors = as.factor(ppe_visitors),
    seperation_between_production_units = as.factor(seperation_between_production_units),
    seperation_within_production_units = as.factor(seperation_within_production_units),
    seperation_quarantaine_area = as.factor(seperation_quarantaine_area),
    bird_nests = as.factor(bird_nests),
    verification_outside_area_contact_poultry = as.factor(verification_outside_area_contact_poultry),
    verification_outside_area_contact_wild_birds = as.factor(verification_outside_area_contact_wild_birds),
    verification_contact_poultry_stable = as.factor(verification_contact_poultry_stable),
    verification_isolation_respiratory_dieseased_pigs = as.factor(verification_isolation_respiratory_dieseased_pigs),
    farrowing_air_quality = as.factor(farrowing_air_quality),
    farrowing_airspace_with_other_agegroup = as.factor(farrowing_airspace_with_other_agegroup),
    ai_sows_air_quality = as.factor(ai_sows_air_quality),
    ai_airspace_with_other_agegroup = as.factor(ai_airspace_with_other_agegroup),
    gilts_reduced_general_wellbeing = as.factor(gilts_reduced_general_wellbeing),
    gilts_air_quality = as.factor(gilts_air_quality),
    gilts_airspace_with_other_agegroup = as.factor(gilts_airspace_with_other_agegroup),
    weaners_air_quality = as.factor(weaners_air_quality),
    weaners_airspace_with_other_agegroup = as.factor(weaners_airspace_with_other_agegroup),
    fatteners_air_quality = as.factor(fatteners_air_quality),
    fatteners_airspace_with_other_agegroup = as.factor(fatteners_airspace_with_other_agegroup),
    farrowing_sows_rectal_temperature = as.factor(farrowing_sows_rectal_temperature),
    caretaker_contact_other_pigs = as.factor(caretaker_contact_other_pigs),
    caretaker_contact_poultry = as.factor(caretaker_contact_poultry),
    production_cycle = factor(
      case_when(
        production_cycle %in% c(3,2,1,6,5,4,7) ~ as.character(production_cycle),
        TRUE ~ "other"
      ),
      levels = c("3","2","1","6","5","4","7","other")
    ),
    quarantaine_time = factor(
      case_when(
        quarantaine_time %in% c(16,4,3,6,5,20,2) ~ as.character(quarantaine_time),
        !is.na(quarantaine_time) ~ "other"
       
      ),
      levels = c("16", "4", "3", "6", "5", "20", "2", "other")
    ),
    source_of_contact = trimws(source_of_contact),
    source_of_contact_grouped = case_when(
      source_of_contact == "Schweineklinik Bern" ~ "Schweineklinik Bern",
      source_of_contact == "Tierarztpraxis Hutter" ~ "Tierarztpraxis Hutter",
      source_of_contact == "Jean-Luc Charbon" ~ "Jean-Luc Charbon",
      source_of_contact == "Vetteam Willisau" ~ "Vetteam Willisau",
      source_of_contact == "Stadtkind im Schweinestall" ~ "Stadtkind im Schweinestall",
      source_of_contact == "Tezet Müllheim" ~ "Tezet Müllheim",
      source_of_contact %in% c("SGD", "Qualiporc", "Profera", "Krieger AG Lüftungen") ~ "Company/Association",
      source_of_contact %in% c("Direkte Anfrage", "Philipp Egli", "Samuel Ritter", "Self report", "Christoph Meister", "Toni Schönbächler") ~ "Private Contact",
      TRUE ~ "Other Private Practices"
    ),
    source_of_contact_grouped = factor(
      source_of_contact_grouped,
      levels = c(
        "Schweineklinik Bern", "Tierarztpraxis Hutter", "Jean-Luc Charbon", 
        "Vetteam Willisau", "Stadtkind im Schweinestall", "Tezet Müllheim",
        "Company/Association", "Private Contact", "Other Private Practices"
      )
    ),
    Age_weeks_factor = factor(
      sapply(age_group_weeks, age_weeks_factor),
      levels = c("0-4w", "5-8w", "9-12w", "13+w", "mixed_ages<10w", "mixed_ages>10w")
    ),
    # Use pre-processed respiratory disease outbreak frequencies
    frequency_respi_outbreak = factor(
      df_temp$category,
      levels = c("never", "few pigs affected no outbreak", "once", "1x/year", "2x/year", "3x or more per year")
    ),
    weaners_animals_per_feeding_site_factor = factor(
      case_when(
        is.na(weaners_animals_per_feeding_site) ~ NA_character_,
        grepl("cm/Tier", weaners_animals_per_feeding_site, ignore.case = TRUE) ~ "trough",
        grepl("^[0-9]", weaners_animals_per_feeding_site) ~ as.character(
          cut(
            as.numeric(sub("^([0-9]+\\.?[0-9]*).*", "\\1", weaners_animals_per_feeding_site)),
            breaks = c(-Inf, 20, 30, 40, Inf),
            labels = c("<20", "<30", "<40", ">40"),
            right = FALSE
          )
        ),
        TRUE ~ NA_character_
      ),
      levels = c("<20", "<30", "<40", ">40", "trough")
    ),
    fatteners_feeding_site_per_animal_factor = factor(
      sapply(fatteners_feeding_site_width_per_animal, make_category),
      levels = category_levels
    )
  ) %>%
  # 7b. Logical conversions (robust, explicit)
  mutate(
    Farrowing_on_farm   = as.logical(ifelse(number_suckling_piglets > 0, TRUE, FALSE)),
    Isemination_on_farm = as.logical(ifelse(number_boars > 0, TRUE, FALSE)),
    Gestation_on_farm   = as.logical(ifelse(number_boars > 0, TRUE, FALSE)),
    Weaners_on_farm     = as.logical(ifelse(number_weaners > 0, TRUE, FALSE)),
    Fattening_on_farm   = as.logical(ifelse(number_fattening_pigs > 0, TRUE, FALSE)),
    outside_area_contact_poultry = ifelse(outside_area_contact_poultry == 1, FALSE, TRUE),
    outside_area_contact_wild_birds = ifelse(outside_area_contact_wild_birds == 1, FALSE, TRUE),
    contact_poultry_in_stable = ifelse(contact_poultry_in_stable == 1, FALSE, TRUE),
    caretaker_hands_washed_before_entry = as.logical(caretaker_hands_washed_before_entry),
    respiratory_history_contact_person = ifelse(is.na(respiratory_history_contact_person), FALSE, TRUE),
    caretaker_boot_desinfection = ifelse(caretaker_boot_desinfection == 1, FALSE, TRUE),
    horses_closeby  = make_logical(grepl("\\b2\\b", other_animals)),
    dogs_closeby    = make_logical(grepl("\\b3\\b", other_animals)),
    chicken_closeby = make_logical(grepl("\\b4\\b", other_animals)),
    turkey_closeby  = make_logical(grepl("\\b5\\b", other_animals)),
    cattle_closeby  = make_logical(grepl("\\b6\\b", other_animals)),
    cats_closeby    = make_logical(grepl("\\b7\\b", other_animals)),
    symptom_swine_sneezing        = make_logical(grepl("\\b1\\b", symptom_type_swine)),
    symptom_swine_coughing        = make_logical(grepl("\\b2\\b", symptom_type_swine)),
    symptom_swine_nasal_discharge = make_logical(grepl("\\b3\\b", symptom_type_swine)),
    symptom_swine_fever           = make_logical(grepl("\\b4\\b", symptom_type_swine)),
    symptom_swine_feed_intake_red = make_logical(grepl("\\b5\\b", symptom_type_swine)),
    symptom_swine_apathy          = make_logical(grepl("\\b6\\b", symptom_type_swine)),
    symptom_swine_dyspnoea        = make_logical(grepl("\\b7\\b", symptom_type_swine)),
    symptom_human_sneezing         = make_logical(grepl("\\b1\\b", symptom_type_human)),
    symptom_human_coughing         = make_logical(grepl("\\b2\\b", symptom_type_human)),
    symptom_human_brobchitis       = make_logical(grepl("\\b3\\b", symptom_type_human)),
    symptom_human_pneumonia        = make_logical(grepl("\\b4\\b", symptom_type_human)),
    symptom_human_fever            = make_logical(grepl("\\b5\\b", symptom_type_human)),
    symptom_human_headache         = make_logical(grepl("\\b6\\b", symptom_type_human)),
    symptom_human_myalgia          = make_logical(grepl("\\b7\\b", symptom_type_human)),
    bird_nests = ifelse(bird_nests == 1, FALSE, TRUE),
    chronic_disease_conditon = ifelse(
      is.na(chronic_disease_conditon) | chronic_disease_conditon == "",
      FALSE,
      TRUE
    ),
    farrowings_nest_temperature = ifelse(
      is.na(farrowings_nest_temperature),
      NA,
      farrowings_nest_temperature == "acceptable"
    ),
    SIV_positive = no_positive_pigs > 0
  ) %>%
  # 7c. Integer/Numeric conversions
  mutate(
    caretaker_number = as.integer(as.character(caretaker_number)),
    gilts_delta_room_temperature = as.numeric(gilts_delta_room_temperature),
    farrowing_delta_room_temperature = as.numeric(farrowing_delta_room_temperature),
    visitors_in_stable_recent = as.integer(visitors_in_stable_recent),
    visitors_cumulative_contact_hours = as.integer(visitors_cumulative_contact_hours),
    across(
      intersect(
        c(
          "farm_id", "total_no_of_samples", "no_positive_pigs", "herdsize",
          "number_suckling_piglets", "number_weaners", "number_fattening_pigs", "number_young_sows",
          "number_old_sows", "number_boars",
          "number_of_origins", "number_of_origins_suckiling_piglets", "number_of_origins_weaners", "number_of_origins_fattening_pigs",
          "number_of_origins_young_sows", "number_of_origins_old_sows", "number_of_origins_boars",
          "start_time_current_outbreak", "starting_point_current_disease"
        ), names(.)
      ),
      ~as.integer(.)
    ),
    across(
      intersect(
        c(
          "percentage_positive_pigs", "min_cp", "max_value", "average_cp", "std_dev",
          "farrowing_piglets_sneezing", "farrowing_piglets_coughing",
          "farrowing_piglets_rectal_temperature", "farrowing_delta_room_temperature", "farrowing_airflow",
          "ai_sows_rectal_temperature","ai_sows_delta_room_temperature", "ai_sows_airflow","gilts_rectal_temperature", "gilts_qm_per_animal", "gitls_animals_per_water_source",
          "gilts_delta_room_temperature", "gilts_airflow", "weaners_sneezing", "weaners_coughing", "weaners_rectal_temperature",
          "weaners_qm_per_animal", "weaners_animals_per_water_source", "weaners_delta_room_temperature", "weaners_airflow",
          "fatteners_sneezing", "fatteners_coughing", "fatteners_rectal_temperature", "fatteners_qm_per_animal",
          "fatteners_animals_per_water_source", "fatteners_room_temperature", "fatteners_airflow",
          "percent_diseased_suckling_piglets", "percent_diseased_weaners", "percent_diseased_fatteners",
          "percent_diseased_young_sows", "percent_diseased_old_sows", "percent_diseased_boars",
          "visitors_cumulative_contact_hours", "weaners_reduced_general_wellbeing", "weaners_discharge",
          "fatteners_reduced_general_wellbeing", "fatteners_discharge", "fatteners_delta_room_temperature"
        ), names(.)
      ),
      as.numeric
    )
  ) %>%
  # 7d. Renaming and relocating columns for logical grouping
  rename(
    farrowing_piglet_litters_sneezing_percentage = farrowing_piglets_sneezing,
    farrowing_piglet_litters_coughing_percentage = farrowing_piglets_coughing,
    farrowing_room_temperature = farrowing_delta_room_temperature,
    weaners_room_temperature = weaners_delta_room_temperature,
    farrowing_nest_temperature_ok = farrowings_nest_temperature,
    ai_sows_room_temperature = ai_sows_delta_room_temperature,
    gilts_room_temperature = gilts_delta_room_temperature,
    fatteners_room_temperature = fatteners_delta_room_temperature,
    chronic_disease_condition = chronic_disease_conditon,
    max_cp = max_value, 
    contact_bird_in_stable = contact_poultry_in_stable
  ) %>%
  relocate(
    SIV_positive, .after = farm_id
  ) %>%
  relocate(
    total_no_of_samples, no_positive_pigs, percentage_positive_pigs,
    min_cp, max_cp, average_cp, std_dev, ili_symptoms, symptomatic_report,
    .after = SIV_positive
  ) %>%
  relocate(
    date_sampling, canton, herdsize, production_type,
    Farrowing_on_farm, Isemination_on_farm, Gestation_on_farm, Weaners_on_farm, Fattening_on_farm,
    .after = std_dev
  ) %>%
  relocate(
    weaners_animals_per_feeding_site, .before = weaners_animals_per_water_source
  ) %>%
  relocate(
    weaners_animals_per_feeding_site_factor, .after = weaners_animals_per_feeding_site
  ) %>%
  relocate(
    fatteners_feeding_site_width_per_animal, .before = fatteners_animals_per_water_source
  ) %>%
  relocate(
    fatteners_feeding_site_per_animal_factor, .after = fatteners_feeding_site_width_per_animal
  ) %>%
  relocate(
    horses_closeby, dogs_closeby, chicken_closeby, turkey_closeby, cattle_closeby, cats_closeby, .after = other_animals
  ) %>%
  relocate(
    symptom_swine_sneezing, symptom_swine_coughing, symptom_swine_nasal_discharge,
    symptom_swine_fever, symptom_swine_feed_intake_red, symptom_swine_apathy, symptom_swine_dyspnoea,
    .after = symptom_type_swine
  ) %>%
  relocate(
    symptom_human_sneezing, symptom_human_coughing, symptom_human_brobchitis,
    symptom_human_pneumonia, symptom_human_fever, symptom_human_headache, symptom_human_myalgia,
    .after = symptom_type_human
  ) %>%
  relocate(
    Age_weeks_factor, .after = age_group_weeks
  ) %>%
  relocate(
    frequency_respi_outbreak, .after = frequency_respiratory_disease_outbreaks
  ) %>%
  relocate(
    source_of_contact_grouped, .after = source_of_contact
  ) %>%
  relocate(
    weaners_animals_per_feeding_site_factor, .after = weaners_qm_per_animal
  )

# ---- 9. Save cleaned data to RDS ----
# Uncomment to save data after validation
# saveRDS(df2, file.path(dirs[2], "siv_farms_cleaned.rds"))


# ---- 8. Quick Data Overview ----
# Use these for further inspection and QA
# glimpse(df2)
# DT::datatable(df2)
# utils::View(df2)
# sapply(df2, class)
# rmarkdown::render("eda_report.Rmd", 
                 # output_file = "siv_farms_First_lokk.html", 
                 # output_dir = "04_output", 
                 # envir = globalenv())


# Script metadata
# Generated by: Jonas Alexander Steiner
# Last updated: 2025-07-11




## Creation of df3 after first look on the EDA
# ------------------------------------------------------------------------------
# Best practice annotation:
# This script processes the cleaned SIV farms data (df2) into a new dataframe (df3)
# with domain-specific transformations and column selection, then renders an RMarkdown
# report for descriptive statistics. All code is explicit and reproducible.
# ------------------------------------------------------------------------------
# Author: jonasalexandersteiner
# Date: 2025-07-11
# Project: IVA_Projekt - SIV farms data processing and reporting
# ------------------------------------------------------------------------------
# Prerequisites:
# - df2 must be loaded in the workspace (see data_cleaning_pipeline)
# - Required R packages: tidyverse, rmarkdown, htmlwidgets, DT
# - RMarkdown file 'eda_report2.Rmd' must exist in the working directory
# ------------------------------------------------------------------------------

df3 <- df2 %>%
  # 1. Exclude all rows of Farm_ID 2, 8, and 106 (outliers or problematic farms)
  filter(!(farm_id %in% c(2, 8, 106))) %>%
  
  # 2. Mutations and transformations (see domain documentation for details)
  mutate(
    # Format date_sampling as "MON YYYY" (e.g. "JAN 2024")
    date_sampling = paste(toupper(format(date_sampling, "%b")), format(date_sampling, "%Y")),        
    
    # Canton_factor: aggregate Swiss cantons into study regions
    canton_factor = case_when(
      canton %in% c("Aargau", "Basel Land") ~ "North-West",
      canton %in% c("Bern", "Solothurn") ~ "Berne + Solothurn",
      canton %in% c("Schaffhausen", "Solothurn", "St. Gallen", "Thurgau", "Z\u00fcrich") ~ "East & Zurich",
      canton %in% c("Fribourg", "Waadt", "Jura") ~ "Romandy",
      canton == "Luzern" ~ "Lucerne",
      TRUE ~ as.character(canton)
    ) %>% factor(levels = c(
      "North-West",
      "Berne + Solothurn",
      "East & Zurich",
      "Romandy",
      "Lucerne"
    )),
    
    # Production_type_factor: group production types into 5 categories
    production_type_factor = case_when(
      production_type %in% c(1, 11, 12) ~ 1L,
      production_type  %in% c(2, 4) ~ 2L,
      production_type == 3 ~ 3L,
      production_type %in% c(5, 6, 7, 8) ~ 4L,
      production_type %in% c(9, 10) ~ 5L,
      TRUE ~ NA_integer_
    ) %>% factor(levels = 1:5),
    
    # Replace zeros with NA in all number_* columns
    across(starts_with("number_"), ~na_if(., 0)),
    
    # Simplify production_cycle into factor levels 1/2/3 or 'other'
    production_cycle = factor(
      case_when(
        production_cycle %in% c(1, 2, 3) ~ as.character(production_cycle),
        !is.na(production_cycle) ~ "other",
        TRUE ~ NA_character_
      ),
      levels = c("1", "2", "3", "other")
    ),
    
    # Transform occupancy and cleaning variables to binary/factor (see domain key)
    mode_stable_occupation_ai_centre = case_when(
      is.na(mode_stable_occupation_ai_centre) ~ NA,
      mode_stable_occupation_ai_centre == 1 ~ TRUE,
      TRUE ~ FALSE
    ),
    mode_stable_occupation_gilts_stable = case_when(
      is.na(mode_stable_occupation_gilts_stable) ~ NA,
      mode_stable_occupation_gilts_stable == 6 ~ TRUE,
      TRUE ~ FALSE
    ),
    mode_stable_occupation_farrowing_stable = case_when(
      is.na(mode_stable_occupation_farrowing_stable) ~ NA,
      mode_stable_occupation_farrowing_stable == 1 ~ TRUE,
      TRUE ~ FALSE
    ),
    litter_equalization_farrowing_stable = factor(case_when(
      is.na(litter_equalization_farrowing_stable) ~ NA_character_,
      litter_equalization_farrowing_stable %in% c(1,2,3) ~ "1",
      litter_equalization_farrowing_stable == 4 ~ "2",
      litter_equalization_farrowing_stable == 5 ~ "3",
    ), levels = c("1","2","3")),
    mode_stable_occupation_weaner_stable = case_when(
      is.na(mode_stable_occupation_weaner_stable) ~ NA,
      mode_stable_occupation_weaner_stable == 1 ~ TRUE,
      TRUE ~ FALSE
    ),
    mode_stable_occupation_fattener_stable = case_when(
      is.na(mode_stable_occupation_fattener_stable) ~ NA,
      mode_stable_occupation_fattener_stable == 1 ~ TRUE,
      TRUE ~ FALSE
    ),
    outside_area = if_any(starts_with("outside_area"), ~.x == TRUE),
    cleaning_ai_centre = factor(case_when(
      is.na(cleaning_ai_centre) ~ NA_character_,
      cleaning_ai_centre == 2 ~ "1",
      cleaning_ai_centre == 4 ~ "2",
      cleaning_ai_centre == 5 ~ "3"
    ), levels = c("1","2","3")),
    cleaning_gilts_stable = case_when(
      is.na(cleaning_gilts_stable) ~ NA,
      cleaning_gilts_stable == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    cleaning_farrowing_stable = case_when(
      is.na(cleaning_farrowing_stable) ~ NA,
      cleaning_farrowing_stable == 5 ~ TRUE,
      TRUE ~ FALSE
    ),
    cleaning_weaner_stable = case_when(
      is.na(cleaning_weaner_stable) ~ NA,
      cleaning_weaner_stable == 5 ~ TRUE,
      TRUE ~ FALSE
    ),
    cleaning_fattener_stable = factor(case_when(
      is.na(cleaning_fattener_stable) | cleaning_fattener_stable == 1 ~ NA_character_,
      cleaning_fattener_stable == 2 ~ "1",
      cleaning_fattener_stable %in% c(3,4) ~ "2",
      cleaning_fattener_stable %in% c(5) ~ "3"
    ), levels = c("1","2","3")),
    cleaning_quarantaine = factor(case_when(
      is.na(cleaning_quarantaine) | cleaning_quarantaine == 1 ~ NA_character_,
      cleaning_quarantaine == 2 ~ "1",
      cleaning_quarantaine == 4 ~ "2",
      cleaning_quarantaine == 5 ~ "3"
    ), levels = c("1","2","3")),
    desinfection_ai_centre = case_when(
      is.na(desinfection_ai_centre) ~ NA,
      desinfection_ai_centre == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    desinfection_gilts_stable = case_when(
      is.na(desinfection_gilts_stable) ~ NA,
      desinfection_gilts_stable == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    desinfection_farrowing_stable = factor(case_when(
      is.na(desinfection_farrowing_stable) ~ NA_character_,
      desinfection_farrowing_stable == 2 ~ "1",
      desinfection_farrowing_stable == 4 ~ "2",
      desinfection_farrowing_stable == 5 ~ "3"
    ), levels = c("1", "2", "3")),
    desinfection_weaner_stable = factor(case_when(
      is.na(desinfection_weaner_stable) ~ NA_character_,
      desinfection_weaner_stable == 2 ~ "1",
      desinfection_weaner_stable %in% c(3,4) ~ "2",
      desinfection_weaner_stable == 5 ~ "3"
    ), levels = c("1", "2", "3")),
    desinfection_fattener_stable = case_when(
      is.na(desinfection_fattener_stable) ~ NA,
      desinfection_fattener_stable == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    desinfection_quarantaine = case_when(
      is.na(desinfection_quarantaine) | desinfection_quarantaine == 1 ~ NA,
      desinfection_quarantaine == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    drying_ai_centre = factor(case_when(
      is.na(drying_ai_centre) ~ NA_character_,
      drying_ai_centre == 2 ~ "1",
      drying_ai_centre == 4 ~ "2",
      drying_ai_centre == 5 ~ "3"
    ), levels = c("1", "2", "3")),
    drying_gilts_stable = case_when(
      is.na(drying_gilts_stable) ~ NA,
      drying_gilts_stable == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    drying_farrowing_stable = case_when(
      is.na(drying_farrowing_stable) ~ NA,
      drying_farrowing_stable == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    drying_weaner_stable = case_when(
      is.na(drying_weaner_stable) ~ NA,
      drying_weaner_stable == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    drying_fattener_stable = factor(case_when(
      is.na(drying_fattener_stable) | drying_fattener_stable == 1 ~ NA_character_,
      drying_fattener_stable == 2 ~ "1",
      drying_fattener_stable %in% c(3,4) ~ "2",
      drying_fattener_stable == 5 ~ "3"
    ), levels = c("1", "2", "3")),
    drying_quarantaine = case_when(
      is.na(drying_quarantaine) | drying_quarantaine == 1 ~ NA,
      drying_quarantaine == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    cleaning_desinfection_transport_vehicle = case_when(
      is.na(cleaning_desinfection_transport_vehicle) | cleaning_desinfection_transport_vehicle == 1 ~ NA,
      cleaning_desinfection_transport_vehicle == 5 ~ TRUE,
      cleaning_desinfection_transport_vehicle %in% c(2,4) ~ FALSE
    ),
    cleaning_shipment_area = case_when(
      is.na(cleaning_shipment_area) ~ NA,
      cleaning_shipment_area == 5 ~ TRUE,
      TRUE ~ FALSE
    ),
    caretaker_ppe_stable = case_when(
      caretaker_ppe_stable == 3 ~ TRUE,
      caretaker_ppe_stable %in% c(1, 2) ~ FALSE,
      TRUE ~ NA
    ),
    caretaker_ppe_washing_interval = case_when(
      is.na(caretaker_ppe_washing_interval) | caretaker_ppe_washing_interval == 1 ~ NA,
      caretaker_ppe_washing_interval == 4 ~ FALSE,
      caretaker_ppe_washing_interval == 5 ~ TRUE
    ),
    caretaker_ppe_per_unit = case_when(
      is.na(caretaker_ppe_per_unit) ~ NA,
      caretaker_ppe_per_unit == 1 ~ FALSE,
      caretaker_ppe_per_unit %in% c(2,3) ~ TRUE
    ),
    caretaker_disease_management = case_when(
      is.na(caretaker_disease_management) | caretaker_disease_management == 1 ~ NA,
      caretaker_disease_management == 2 ~ FALSE,
      caretaker_disease_management %in% c(3,4) ~ TRUE
    ),
    caretaker_contact_other_pigs = case_when(
      caretaker_contact_other_pigs %in% c(3, 4) ~ TRUE,
      caretaker_contact_other_pigs %in% c(1, 2) ~ FALSE,
      caretaker_contact_other_pigs == 5 | is.na(caretaker_contact_other_pigs) ~ NA
    ),
    caretaker_contact_poultry = case_when(
      is.na(caretaker_contact_poultry) | caretaker_contact_poultry == 5 ~ NA,
      caretaker_contact_poultry %in% c(3,4) ~ TRUE,
      caretaker_contact_poultry %in% c(1,2) ~ FALSE
    ),
    ppe_visitors = case_when(
      is.na(ppe_visitors) ~ NA,
      ppe_visitors == 3 ~ TRUE,
      ppe_visitors == 1 ~ FALSE
    ),
    visitors_disease_management = case_when(
      is.na(visitors_disease_management) | visitors_disease_management == 1 ~ NA,
      visitors_disease_management %in% c(3,4) ~ TRUE,
      visitors_disease_management == 2 ~ FALSE
    ),
    visitors_contact_other_pigs = case_when(
      visitors_contact_other_pigs == 4 ~ TRUE,
      visitors_contact_other_pigs %in% c(1, 2, 3) ~ FALSE,
      visitors_contact_other_pigs == 5 | is.na(visitors_contact_other_pigs) ~ NA
    ),
    return_to_service_rate = case_when(
      is.na(return_to_service_rate) ~ NA,
      return_to_service_rate == 1 ~ TRUE,
      return_to_service_rate == 2 ~ FALSE
    ),
    farrowing_rate = case_when(
      is.na(farrowing_rate) ~ NA,
      farrowing_rate == 1 ~ TRUE,
      farrowing_rate == 2 ~ FALSE
    ),
    piglets_per_sow_year = case_when(
      is.na(piglets_per_sow_year) ~ NA,
      piglets_per_sow_year == 1 ~ TRUE,
      piglets_per_sow_year == 2 ~ FALSE
    ),
    abortions_per_sow_year = case_when(
      is.na(abortions_per_sow_year) ~ NA,
      abortions_per_sow_year == 1 ~ TRUE,
      abortions_per_sow_year %in% c(2, 3) ~ FALSE
    ),
    piglet_mortality = case_when(
      is.na(piglet_mortality) ~ NA,
      piglet_mortality == 1 ~ TRUE,
      piglet_mortality == 2 ~ FALSE
    ),
    feed_conversion_rate_fatteners = case_when(
      is.na(feed_conversion_rate_fatteners) ~ NA,
      feed_conversion_rate_fatteners == 1 ~ TRUE,
      feed_conversion_rate_fatteners == 2 ~ FALSE
    ),
    respiratory_history_swine = case_when(
      respiratory_history_swine == 1 ~ NA_integer_,
      respiratory_history_swine == 2 ~ 1,
      respiratory_history_swine %in% c(3, 4) ~ 2,
      respiratory_history_swine == 5 ~ 3
    ),respiratory_history_swine = factor(respiratory_history_swine),
    time_respiratory_disease = case_when(
      is.na(time_respiratory_disease) | time_respiratory_disease == 1 ~ NA,
      time_respiratory_disease %in% c(2, 3) ~ FALSE,
      time_respiratory_disease == 4 ~ TRUE
    ),
    suckling_piglets_diseased = percent_diseased_suckling_piglets != 0,
    weaners_diseased = percent_diseased_weaners != 0,
    fatteners_diseased = percent_diseased_fatteners != 0,
    young_sows_diseased = percent_diseased_young_sows != 0,
    old_sows_diseased = percent_diseased_old_sows != 0,
    boars_diseased = percent_diseased_boars != 0,
    report_killed_suckling_piglets = percent_killed_suckling_piglets != 0,
    report_killed_weaners = percent_killed_weaners != 0,
    report_killed_fatteners = percent_killed_fatteners != 0,
    report_killed_young_sows = percent_killed_young_sows != 0,
    report_killed_old_sows = percent_killed_old_sows != 0,
    starting_point_current_disease = na_if(starting_point_current_disease, 0),
    symptom_severity = case_when(
      is.na(symptom_severity) ~ NA,
      symptom_severity == 1 ~ FALSE,
      symptom_severity %in% c(2,3) ~ TRUE
    ),
    seperation_between_production_units = factor(
      case_when(
        seperation_between_production_units == 2 ~ "1",
        seperation_between_production_units == 3 ~ "2",
        seperation_between_production_units %in% c(1, 4) ~ "3",
        TRUE ~ NA_character_
      ),
      levels = c("1", "2", "3"),
    ),
    seperation_within_production_units = factor(
      case_when(
        seperation_within_production_units == 2 ~ "1",
        seperation_within_production_units == 3 ~ "2",
        seperation_within_production_units %in% c(1, 4) ~ "3",
        TRUE ~ NA_character_
      ),
      levels = c("1", "2", "3"),
    ),
    seperation_quarantaine_area = factor(
      case_when(
        seperation_quarantaine_area == 2 ~ "1",
        seperation_quarantaine_area == 3 ~ "2",
        seperation_quarantaine_area %in% c(1, 4) ~ "3",
        TRUE ~ NA_character_
      ),
      levels = c("1", "2", "3"),
    ),
    verification_outside_area_contact_poultry = case_when(
      is.na(verification_outside_area_contact_poultry) ~ NA,
      verification_outside_area_contact_poultry == 1 ~ FALSE,
      verification_outside_area_contact_poultry %in% c(3,4) ~ TRUE
    ),
    verification_outside_area_contact_wild_birds = case_when(
      is.na(verification_outside_area_contact_wild_birds) ~ NA,
      verification_outside_area_contact_wild_birds == 1 ~ FALSE,
      verification_outside_area_contact_wild_birds %in% c(3,4) ~ TRUE
    ),
    verification_contact_poultry_stable = case_when(
      is.na(verification_contact_poultry_stable) ~ NA,
      verification_contact_poultry_stable == 1 ~ FALSE,
      verification_contact_poultry_stable %in% c(3,4) ~ TRUE
    ),
    farrowing_sows_coughing = farrowing_sows_coughing != 0,
    farrowing_piglets_reduced_general_wellbeing = farrowing_piglets_reduced_general_wellbeing != 0,
    weaners_reduced_general_wellbeing = weaners_reduced_general_wellbeing != 0,
    weaners_discharge = weaners_discharge != 0,
    fatteners_reduced_general_wellbeing = fatteners_reduced_general_wellbeing != 0,
    fatteners_discharge = fatteners_discharge != 0,
    farrowing_airspace_with_other_agegroup = factor(case_when(
      is.na(farrowing_airspace_with_other_agegroup) ~ NA_character_,
      farrowing_airspace_with_other_agegroup == 2 ~ "1",
      farrowing_airspace_with_other_agegroup == 3 ~ "2",
      farrowing_airspace_with_other_agegroup == 4 ~ "3"
    ), levels = c("1", "2", "3")),
    ai_airspace_with_other_agegroup = factor(case_when(
      is.na(ai_airspace_with_other_agegroup) ~ NA_character_,
      ai_airspace_with_other_agegroup == 2 ~ "1",
      ai_airspace_with_other_agegroup == 3 ~ "2",
      ai_airspace_with_other_agegroup == 4 ~ "3"
    ), levels = c("1", "2", "3")),
    gilts_airspace_with_other_agegroup = factor(case_when(
      is.na(gilts_airspace_with_other_agegroup) ~ NA_character_,
      gilts_airspace_with_other_agegroup == 2 ~ "1",
      gilts_airspace_with_other_agegroup == 3 ~ "2",
      gilts_airspace_with_other_agegroup == 4 ~ "3"
    ), levels = c("1", "2", "3"))
  ) %>%
  # 3. Remove columns not needed for report or analysis
  dplyr::select(
    -any_of(c(
      "canton", "sgd_qgs", "production_type", "source_of_contact", "frequency_respiratory_disease_outbreaks",
      "notes", "other_animals", "quarantaine_suckling_piglets", "quarantaine_weaners", "quarantaine_fatteners", 
      "quarantaine_young_sows", "quarantaine_old_sows", "quarantaine_boars", "visitors_in_stable",
      "visitors_contact_poultry", "isolation_respiratory_dieseased_pigs", "growing_rate_weaners", 
      "growing_rate_fatteners", "feed_conversion_rate_weaners", "influenza_diagnosis_hum_an",
      "antiviral_treatment", "fatteners_feeding_site_width_per_animal", 
      "verification_isolation_respiratory_dieseased_pigs",
      "farrowing_sows_reduced_general_wellbeing", "farrowing_sows_sneezing", "farrowing_sows_nasal_discharge",
      "farrowing_sows_rectal_temperature", "farrowing_piglets_nasal_discharge", "farrowing_piglets_rectal_temperature",
      "ai_sows_reduced_general_wellbeing", "ai_sows_sneezing", "ai_sows_coughing", "ai_sows_discharge", "ai_sows_rectal_temperature",
      "gilts_reduced_general_wellbeing", "gilts_sneezing", "gilts_coughing", "gilts_discharge", "gilts_rectal_temperature",
      "outbreak_since_examination_description", "symptom_type_swine", "symptom_type_human", "percent_killed_boars",
      "percent_diseased_suckling_piglets", "percent_diseased_weaners", "percent_diseased_fatteners",
      "percent_diseased_young_sows", "percent_diseased_old_sows", "percent_diseased_boars",
      "percent_killed_suckling_piglets", "percent_killed_weaners", "percent_killed_fatteners",
      "percent_killed_young_sows", "percent_killed_old_sows", "age_group_weeks", "weaners_animals_per_feeding_site"
    )),
    -starts_with("number_of_origins_"),
    -matches("^number_of_origins$")
  )%>%
  # 4. Relocate new or renamed variables directly after old ones for clarity
  relocate(
    canton_factor, .after = date_sampling
  ) %>%
  relocate(
    production_type_factor, .after = herdsize
  ) %>%
  relocate(
    suckling_piglets_diseased, .after = outbreak_since_examination
  ) %>%
  relocate(
    weaners_diseased, .after = suckling_piglets_diseased
  ) %>%
  relocate(
    fatteners_diseased, .after = weaners_diseased
  ) %>%
  relocate(
    young_sows_diseased, .after = fatteners_diseased
  ) %>%
  relocate(
    old_sows_diseased, .after =  young_sows_diseased
  ) %>%
  relocate(
    boars_diseased, .after = old_sows_diseased
  ) %>%
  relocate(
    report_killed_suckling_piglets, .after =  boars_diseased
  ) %>%
  relocate(
    report_killed_weaners, .after = report_killed_suckling_piglets
  ) %>%
  relocate(
    report_killed_fatteners, .after = report_killed_weaners
  ) %>%
  relocate(
    report_killed_young_sows, .after = report_killed_fatteners
  ) %>%
  relocate(
    report_killed_old_sows, .after = report_killed_young_sows
  ) %>%
  relocate(
    outside_area, .before = outside_area_ai_centre
  )

# ------------------------------------------------------------------------------
# Quick overview (optional: uncomment for interactive use)
# glimpse(df3)
# DT::datatable(df3) %>%
#   htmlwidgets::saveWidget("df3_table.html")
# sapply(df3, class)
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Render R Markdown for descriptive statistics report.
# - The report 'eda_report2.Rmd' uses df3 from the global environment.
# - Output file is saved in the '04_output' folder.
# ------------------------------------------------------------------------------
#rmarkdown::render(
 # "eda_report2.Rmd", 
  #output_file = "siv_farms_descriptive_stats.html", 
  #output_dir = "04_output", 
  #envir = globalenv()
#)

# =====================================================================
# VISUAL SUMMARY PLOTS FOR KEY VARIABLES IN df2
# ---------------------------------------------------------------------
# Creates and saves (in output_folder):
#   - Pie charts for SIV_positive, symptomatic_report, ili_symptoms (TRUE vs FALSE, percent labels)
#   - Bar chart for no_positive_pigs (excluding 0)
#   - Whisker (box) plot for average_cp, stratified by ili_symptoms (TRUE/FALSE)
# =====================================================================
# Best Practice:
# - Use tidy evaluation (.data[[var]]) in ggplot2 aesthetics (never aes_string).
# - Always filter out NA, Inf, -Inf before plotting continuous variables.
# - Use colorblind-friendly palettes for categorical variables.
# - Save plots in a reproducible output folder.
# - Modular plotting functions for easy extension.
# =====================================================================

library(ggplot2)
library(dplyr)
library(scales)

# ------------------ User-Defined Output Folder -----------------------
output_folder <- "04_output"
if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

# ------------------ PIE CHART FUNCTION -------------------------------
# Plots TRUE vs FALSE proportions, labels percent on each slice.
# - Uses factor conversion for logicals (ensures TRUE, FALSE order).
# - Handles NA as a separate slice if present.
# - Colorblind-friendly palette.
save_pie_chart <- function(df, var, filename, title) {
  tab <- df %>%
    mutate(!!var := factor(.data[[var]], levels = c(TRUE, FALSE))) %>%
    group_by(.data[[var]]) %>%
    summarise(n = n()) %>%
    mutate(
      perc = n / sum(n) * 100,
      label = paste0(ifelse(is.na(.data[[var]]), "NA", as.character(.data[[var]])), "\n", sprintf("%.1f%%", perc))
    )
  gg <- ggplot(tab, aes(x = "", y = perc, fill = .data[[var]])) +
    geom_col(width = 1) +
    coord_polar(theta = "y") +
    geom_text(aes(label = label), position = position_stack(vjust = 0.5), size = 6) +
    scale_fill_manual(values = c("TRUE"="#4DAF4A", "FALSE"="#E41A1C", "NA"="#999999")) +
    labs(title = title, fill = var) +
    theme_void() +
    theme(plot.title = element_text(hjust = 0.5, size = 16))
  ggsave(filename, gg, width = 6, height = 6)
}

# ------------------ BAR CHART FUNCTION -------------------------------
# Plots counts for non-zero values of a discrete variable.
# - Uses tidy evaluation for aesthetics.
# - Excludes zero values for cleaner presentation.
# - Handles missing values robustly.
save_bar_chart <- function(df, var, filename, title) {
  tab <- df %>%
    filter(.data[[var]] > 0 & !is.na(.data[[var]])) %>%
    count(.data[[var]])
  gg <- ggplot(tab, aes(x = .data[[var]], y = n)) +
    geom_bar(stat = "identity", fill = "#377EB8") +
    labs(title = title, x = var, y = "Count") +
    theme_minimal(base_size = 16)
  ggsave(filename, gg, width = 8, height = 6)
}

# ------------------ WHISKER (BOX) PLOT FUNCTION ----------------------
# Compares distributions of a continuous variable by binary grouping.
# - Filters out non-finite values (best practice for boxplots).
# - Ensures group_var is a factor with TRUE, FALSE order.
# - Colorblind-friendly fill.
save_whisker_plot <- function(df, value_var, group_var, filename, title) {
  df <- df %>%
    filter(is.finite(.data[[value_var]])) %>%
    mutate(!!group_var := factor(.data[[group_var]], levels = c(TRUE, FALSE)))
  gg <- ggplot(df, aes(x = .data[[group_var]], y = .data[[value_var]], fill = .data[[group_var]])) +
    geom_boxplot(width = 0.6, outlier.colour = "gray", outlier.shape = 16) +
    scale_fill_manual(values = c("TRUE"="#4DAF4A", "FALSE"="#E41A1C")) +
    labs(title = title, x = group_var, y = value_var) +
    theme_minimal(base_size = 16)
  ggsave(filename, gg, width = 8, height = 6)
}

# =====================================================================
# ======================= CREATE AND SAVE PLOTS =======================
# =====================================================================

# ------------------ PIE CHARTS FOR TRUE/FALSE ------------------------
save_pie_chart(
  df2, "SIV_positive",
  file.path(output_folder, "pie_SIV_positive.png"),
  "SIV_positive: TRUE vs FALSE"
)
save_pie_chart(
  df2, "symptomatic_report",
  file.path(output_folder, "pie_symptomatic_report.png"),
  "symptomatic_report: TRUE vs FALSE"
)
save_pie_chart(
  df2, "ili_symptoms",
  file.path(output_folder, "pie_ili_symptoms.png"),
  "ili_symptoms: TRUE vs FALSE"
)

# ------------------ BAR CHART FOR no_positive_pigs -------------------
save_bar_chart(
  df2, "no_positive_pigs",
  file.path(output_folder, "bar_no_positive_pigs.png"),
  "Number of Positive Pigs (Excluding 0)"
)

# ---- WHISKER PLOT FOR average_cp STRATIFIED BY ili_symptoms ---------
save_whisker_plot(
  df2, "average_cp", "ili_symptoms",
  file.path(output_folder, "boxplot_average_cp_by_ili_symptoms.png"),
  "Average CP Stratified by ili_symptoms"
)

# =====================================================================
# END OF SCRIPT
# =====================================================================
# - All plots saved as PNGs in the output folder.
# - Functions are robust to missing/non-finite values.
# - Tidy evaluation ensures compatibility with latest ggplot2.
# - Color palettes chosen for clarity and accessibility.
# - Modular functions enable easy extension for more plots or variables.
# - All outputs are ready for publication or reporting.


# =====================================================================
# INFERENTIAL ANALYSIS: SIV_positive Only (Association + Correlation)
# ---------------------------------------------------------------------
# For SIV_positive outcome:
#   - For each subset (Husbandry, Animals, Environment, Human)
#     - Univariate association (FDR correction, interactive tables, highlight significant)
#     - Custom statistical test assignment per variable
# Output: association_SIV_positive_by_subset.html
#
# For all variables (not just significant), show correlation with SIV_positive:
#     - Spearman (numeric/binary pairs)
#     - Cramér's V (categorical pairs with >2 levels)
#     - Show: p-value, FDR-p, correlation coefficient, N
# Output: correlation_SIV_positive_by_subset.html
# =====================================================================

library(dplyr)
library(purrr)
library(DT)
library(htmltools)

# ------------------ Variable and Subset Definitions -----------------------
exclude_vars_base <- c(
  "farm_id", "total_no_of_samples", "no_positive_pigs",
  "percentage_positive_pigs", "min_cp", "max_cp", "average_cp", "std_dev",
  "date_sampling", "source_of_contact_grouped"
)
overview_dependent_vars <- c(
  "quarantaine_time", "outside_area_ai_centre", "outside_area_gilts_stable",
  "outside_area_farrowing_stable", "outside_area_weaner_stable", "outside_area_fattener_stable"
)
make_exclude_vars <- function(outcome_var) unique(c(exclude_vars_base, overview_dependent_vars, outcome_var))

get_husbandry_vars <- function(exclude_vars) setdiff(
  c("symptomatic_report", "canton_factor", "herdsize", "production_type_factor",
    "Farrowing_on_farm", "Isemination_on_farm", "Gestation_on_farm", "Weaners_on_farm", "Fattening_on_farm",
    "horses_closeby", "dogs_closeby", "chicken_closeby", "turkey_closeby",
    "cattle_closeby", "cats_closeby", "other_pigs_closeby", "other_poultry_closeby",
    "number_suckling_piglets", "number_weaners", "number_fattening_pigs", "number_young_sows",
    "number_old_sows", "number_boars", "number_of_origins", "quarantaine_concept",
    "quarantaine_in_herd_contact", "herds_of_origin_respiratory_symptoms", "herds_of_origin_influenza_diagnosis",
    "production_cycle", "mode_stable_occupation_ai_centre", "mode_stable_occupation_gilts_stable",
    "mode_stable_occupation_farrowing_stable", "litter_equalization_farrowing_stable",
    "mode_stable_occupation_weaner_stable", "mode_stable_occupation_fattener_stable",
    "passing_through_other_age_group", "outside_area",
    "outside_area_contact_poultry", "outside_area_contact_wild_birds", "outside_area_contact_wild_boars",
    "contact_bird_in_stable", "cleaning_ai_centre", "cleaning_gilts_stable", "cleaning_farrowing_stable",
    "cleaning_weaner_stable", "cleaning_fattener_stable", "cleaning_quarantaine", "desinfection_ai_centre",
    "desinfection_gilts_stable", "desinfection_farrowing_stable", "desinfection_weaner_stable",
    "desinfection_fattener_stable", "desinfection_quarantaine", "drying_ai_centre", "drying_gilts_stable",
    "drying_farrowing_stable", "drying_weaner_stable", "drying_fattener_stable", "drying_quarantaine",
    "cleaning_desinfection_transport_vehicle", "cleaning_shipment_area", "caretaker_type", "caretaker_number",
    "caretaker_ppe_stable", "caretaker_ppe_washing_interval", "caretaker_ppe_per_unit", "caretaker_per_unit",
    "caretaker_work_flow_hygiene_between_units", "caretaker_entry_ppe_only", "caretaker_disease_management",
    "caretaker_hands_washed_before_entry", "caretaker_boot_desinfection", "caretaker_contact_other_pigs",
    "caretaker_contact_poultry", "visitors_in_stable_recent", "visitors_cumulative_contact_hours",
    "visitors_list", "ppe_visitors", "visitors_hands_washed_before_entry", "visitors_disease_management",
    "visitors_contact_other_pigs", "visitors_respiratory_symptoms", "seperation_between_production_units",
    "seperation_within_production_units", "seperation_quarantaine_area", "bird_nests",
    "verification_outside_area_contact_poultry", "verification_outside_area_contact_wild_birds",
    "verification_contact_poultry_stable", "verification_outside_area_contact_wild_boars",
    "farrowing_airspace_with_other_agegroup", "ai_airspace_with_other_agegroup",
    "gilts_airspace_with_other_agegroup", "weaners_airspace_with_other_agegroup",
    "fatteners_airspace_with_other_agegroup", "vet_consultation", "influenza_diagnosis", "influenza_vaccination"),
  exclude_vars)
get_animal_vars <- function(exclude_vars) setdiff(
  c("ili_symptoms", "return_to_service_rate", "farrowing_rate", "piglets_per_sow_year",
    "abortions_per_sow_year", "piglet_mortality", "feed_conversion_rate_fatteners",
    "respiratory_history_swine", "time_respiratory_disease", "frequency_respi_outbreak",
    "start_time_current_outbreak", "outbreak_since_examination", "suckling_piglets_diseased",
    "weaners_diseased", "fatteners_diseased", "young_sows_diseased", "old_sows_diseased",
    "boars_diseased", "report_killed_suckling_piglets", "report_killed_weaners",
    "report_killed_fatteners", "report_killed_young_sows", "report_killed_old_sows",
    "symptom_swine_sneezing", "symptom_swine_coughing", "symptom_swine_nasal_discharge",
    "symptom_swine_fever", "symptom_swine_feed_intake_red", "symptom_swine_apathy",
    "symptom_swine_dyspnoea", "Age_weeks_factor", "farrowing_sows_coughing",
    "farrowing_piglets_reduced_general_wellbeing", "farrowing_piglet_litters_sneezing_percentage",
    "farrowing_piglet_litters_coughing_percentage", "weaners_reduced_general_wellbeing",
    "weaners_sneezing", "weaners_coughing", "weaners_discharge", "weaners_rectal_temperature",
    "fatteners_rectal_temperature", "fatteners_reduced_general_wellbeing", "fatteners_sneezing",
    "fatteners_coughing", "fatteners_discharge"),
  exclude_vars)
get_environment_vars <- function(exclude_vars) setdiff(
  c("farrowing_room_temperature", "farrowing_nest_temperature_ok", "farrowing_airflow",
    "farrowing_air_quality", "ai_sows_room_temperature", "ai_sows_airflow", "ai_sows_air_quality",
    "gilts_qm_per_animal", "gitls_animals_per_water_source", "gilts_room_temperature",
    "gilts_airflow", "gilts_air_quality", "weaners_qm_per_animal", "weaners_animals_per_feeding_site_factor",
    "weaners_animals_per_water_source", "weaners_room_temperature", "weaners_airflow",
    "weaners_air_quality", "fatteners_qm_per_animal", "fatteners_feeding_site_per_animal_factor",
    "fatteners_animals_per_water_source", "fatteners_room_temperature", "fatteners_airflow",
    "fatteners_air_quality"),
  exclude_vars)
get_human_vars <- function(exclude_vars) setdiff(
  c("respiratory_history_human", "respiratory_history_contact_person", "starting_point_current_disease",
    "symptom_human_sneezing", "symptom_human_coughing", "symptom_human_brobchitis",
    "symptom_human_pneumonia", "symptom_human_fever", "symptom_human_headache",
    "symptom_human_myalgia", "symptom_severity", "physician_consultation", "flu_vaccination",
    "flu_vaccination_contacts", "chronic_disease_condition", "smoker"),
  exclude_vars)

# ----- Custom statistical test assignment for continuous vars -------
wilcox_vars <- c(
  "number_fattening_pigs","number_boars","number_of_origins",
  "production_cycle","caretaker_number","visitors_in_stable_recent","visitors_cumulative_contact_hours",
  "start_time_current_outbreak","farrowing_airflow","ai_sows_airflow",
  "gilts_qm_per_animal","gilts_airflow",
  "weaners_sneezing","weaners_coughing","weaners_rectal_temperature","weaners_qm_per_animal",
  "weaners_animals_per_feeding_site_factor","weaners_animals_per_water_source","weaners_airflow","weaners_air_quality","weaners_airspace_with_other_agegroup",
  "fatteners_reduced_general_wellbeing","fatteners_sneezing","fatteners_coughing","fatteners_discharge","fatteners_airflow","fatteners_air_quality","fatteners_airspace_with_other_agegroup",
  "fatteners_feeding_site_per_animal_factor",
  "starting_point_current_disease",
  "farrowing_piglet_litters_sneezing_percentage",
  "farrowing_piglet_litters_coughing_percentage"
)
ttest_vars <- c(
  "herdsize","number_suckling_piglets","number_weaners","number_young_sows","number_old_sows",
  "gitls_animals_per_water_source","gilts_room_temperature",
  "weaners_room_temperature","fatteners_rectal_temperature","fatteners_qm_per_animal",
  "fatteners_animals_per_water_source","fatteners_room_temperature",
  "farrowing_room_temperature","ai_sows_room_temperature"
)

chisq_or_fisher <- function(x, y) {
  tbl <- table(x, y)
  if (any(dim(tbl) < 2) || sum(tbl) == 0) return(list(test = "Not analyzable", pval = NA))
  expected <- outer(rowSums(tbl), colSums(tbl)) / sum(tbl)
  if (any(is.na(expected))) return(list(test = "Not analyzable", pval = NA))
  if (any(expected < 5, na.rm = TRUE)) {
    pval <- tryCatch(fisher.test(tbl)$p.value, error = function(e) NA)
    return(list(test = "Fisher's exact", pval = pval))
  } else {
    pval <- tryCatch(chisq.test(tbl)$p.value, error = function(e) NA)
    return(list(test = "Chi-squared", pval = pval))
  }
}

get_num_test <- function(x, y, var_name) {
  idx <- rep(TRUE, length(x))
  # Outlier removal for specific vars (domain knowledge)
  if (var_name == "herdsize") idx <- x != max(x, na.rm=TRUE)
  if (var_name == "number_suckling_piglets") idx <- x != max(x, na.rm=TRUE)
  if (var_name == "number_weaners") idx <- x <= 1000
  if (var_name == "number_young_sows") idx <- x <= 100
  if (var_name == "number_old_sows") idx <- x <= 400
  if (var_name == "gitls_animals_per_water_source") idx <- x <= 25
  if (var_name == "fatteners_qm_per_animal") idx <- x <= 4
  if (var_name == "fatteners_animals_per_water_source") idx <- x <= 30
  x <- x[idx]; y <- y[idx]
  if (var_name %in% wilcox_vars) {
    pval <- tryCatch(wilcox.test(x ~ y, exact = FALSE)$p.value, error = function(e) NA)
    return(list(test = "Wilcoxon Mann-Whitney", pval = pval))
  } else if (var_name %in% ttest_vars) {
    pval <- tryCatch(t.test(x ~ y)$p.value, error = function(e) NA)
    return(list(test = "t-test", pval = pval))
  } else {
    return(list(test = "Not analyzable", pval = NA))
  }
}

perform_test <- function(var, outcome, var_name) {
  if (is.null(var) || length(var) == 0 || all(is.na(var))) {
    return(list(variable = var_name, class = NA, levels = NA, pct_NA = 100, test = "Not analyzable", pval = NA, N = 0))
  }
  df <- data.frame(x = var, y = outcome)
  df <- df[complete.cases(df), ]
  N <- nrow(df)
  if (N == 0) {
    return(list(variable = var_name, class = class(var)[1], levels = NA, pct_NA = 100, test = "Not analyzable", pval = NA, N = 0))
  }
  df$y <- factor(df$y, levels = c(FALSE, TRUE))
  if (is.logical(df$x)) df$x <- factor(df$x, levels = c(FALSE, TRUE))
  na_pct <- mean(is.na(var)) * 100
  var_class <- class(var)
  if (is.factor(var)) {
    var_levels <- paste0(levels(var), collapse = ", ")
  } else if (is.logical(var)) {
    var_levels <- "FALSE, TRUE"
  } else {
    var_levels <- "continuous"
  }
  if (is.factor(var) | is.logical(var)) {
    res <- chisq_or_fisher(df$x, df$y)
  } else if (is.numeric(var)) {
    res <- get_num_test(df$x, df$y, var_name)
  } else {
    res <- list(test = "Not analyzable", pval = NA)
  }
  list(variable = var_name, class = var_class[1], levels = var_levels, pct_NA = round(na_pct, 1), test = res$test, pval = res$pval, N = N)
}

analyze_subset <- function(var_names, subset_name, df, outcome_var) {
  univ_results <- purrr::map(var_names, ~perform_test(df[[.x]], df[[outcome_var]], .x)) %>%
    dplyr::bind_rows()
  univ_results$pval_fdr <- p.adjust(univ_results$pval, method = "fdr")
  univ_results$subset <- subset_name
  univ_results
}

highlight_fdr <- function(pval_fdr) {
  if (is.na(pval_fdr)) return("")
  if (pval_fdr <= 0.1) {
    return(sprintf('<span style="color:green;font-weight:bold;">%.3g</span>', pval_fdr))
  } else {
    return(sprintf('%.3g', pval_fdr))
  }
}
make_dt <- function(results, subset_name, outcome_name) {
  results %>%
    mutate(
      pval = signif(pval, 3),
      pval_fdr_html = purrr::map_chr(pval_fdr, highlight_fdr)
    ) %>%
    dplyr::select(variable, class, levels, pct_NA, test, N, pval, pval_fdr_html) %>%
    datatable(
      caption = htmltools::tags$caption(
        style = 'caption-side: top; text-align: left;',
        htmltools::tags$strong(
          paste0(subset_name, ": Association with ", outcome_name, " (univariate only)")
        ),
        htmltools::tags$br(),
        htmltools::tags$span(
          style = "font-size: 0.95em; color: #555;",
          "Statistical tests: See 'Test' column. Categorical/discrete: chi-squared or Fisher's exact; continuous: t-test or Wilcoxon.",
          htmltools::tags$br(),
          htmltools::tags$strong(
            "p_FDR: Benjamini-Hochberg FDR correction (values ≤ 0.1 highlighted in green)."
          ),
          htmltools::tags$br(),
          "Missing values: See '% NA'. Outliers may be removed for specific variables (see code)."
        )
      ),
      filter = "top", rownames = FALSE, escape = FALSE,
      options = list(pageLength = 25, autoWidth = TRUE),
      colnames = c("Variable", "Class", "Levels", "% NA", "Test", "N", "p-Value", "p_FDR")
    )
}

# ---------------- Correlation for ALL Variables --------------------
spearman_cor <- function(x, y) {
  idx <- complete.cases(x, y)
  x <- x[idx]; y <- y[idx]
  n <- length(x)
  if (n < 3) return(list(rho=NA, pval=NA, n=n))
  ct <- suppressWarnings(cor.test(x, y, method = "spearman"))
  return(list(rho=ct$estimate, pval=ct$p.value, n=n))
}
cramers_v <- function(x, y) {
  idx <- complete.cases(x, y)
  x <- x[idx]; y <- y[idx]
  n <- length(x)
  if (n < 3) return(list(V=NA, pval=NA, n=n))
  tbl <- table(x, y)
  if (any(dim(tbl) < 2) || sum(tbl) == 0) return(list(V=NA, pval=NA, n=n))
  chi2test <- tryCatch(chisq.test(tbl, correct = FALSE), error = function(e) NULL)
  if (is.null(chi2test)) return(list(V=NA, pval=NA, n=n))
  chi2 <- chi2test$statistic
  k <- min(nrow(tbl), ncol(tbl))
  V <- sqrt(chi2 / (n * (k - 1)))
  pval <- chi2test$p.value
  return(list(V=as.numeric(V), pval=pval, n=n))
}
is_binary <- function(x) {
  (is.factor(x) && length(levels(x)) == 2) ||
    (is.logical(x)) ||
    (is.numeric(x) && length(unique(na.omit(x))) == 2)
}
is_numeric <- function(x) is.numeric(x)
is_categorical <- function(x) is.factor(x) && length(levels(x)) > 2

get_var_correlation <- function(var_name, df) {
  x <- df[[var_name]]
  y <- df[["SIV_positive"]]
  idx <- complete.cases(x, y)
  x <- x[idx]; y <- y[idx]
  n <- length(x)
  # If insufficient data, return NA for all
  if (n < 3) {
    return(list(
      coef = NA, pval = NA, N = n, type = "", html = '<span style="color:gray;" title="N too small">Insufficient data</span>'
    ))
  }
  # Try to determine type
  if ((is_numeric(x) || is_binary(x)) && !is_categorical(x)) {
    # Spearman rank correlation for numeric/binary
    if (is.logical(x)) x <- as.numeric(x)
    if (is.factor(x) && length(levels(x)) == 2) x <- as.numeric(x) - 1
    if (is.logical(y)) y <- as.numeric(y)
    if (is.factor(y) && length(levels(y)) == 2) y <- as.numeric(y) - 1
    res <- spearman_cor(x, y)
    coef <- as.numeric(res$rho)
    pval <- res$pval
    html <- sprintf("%.3f", coef)
    type <- "Spearman"
  } else {
    # Cramér's V for categorical (>2 levels); use chi2 test p-value
    res <- cramers_v(x, y)
    coef <- as.numeric(res$V)
    pval <- res$pval
    html <- sprintf("%.3f", coef)
    type <- "Cramér's V"
  }
  list(
    coef = coef,
    pval = pval,
    N = n,
    type = type,
    html = html
  )
}

make_correlation_results <- function(results, subset_name, df) {
  all_vars <- results$variable
  corrs <- lapply(all_vars, function(v) {
    cor_res <- get_var_correlation(v, df)
    data.frame(
      variable = v,
      N = cor_res$N,
      coef_html = cor_res$html,
      coef = cor_res$coef,
      pval = cor_res$pval,
      test = cor_res$type,
      stringsAsFactors = FALSE
    )
  })
  results_df <- bind_rows(corrs)
  results_df$pval_fdr <- p.adjust(results_df$pval, method = "fdr")
  results_df
}

make_corr_dt <- function(results, subset_name) {
  results %>%
    mutate(
      pval = ifelse(is.na(pval), "", signif(pval, 3)),
      pval_fdr = ifelse(is.na(pval_fdr), "", signif(pval_fdr, 3)),
      coef_html = coef_html
    ) %>%
    dplyr::select(variable, N, coef_html, pval, pval_fdr, test) %>%
    datatable(
      caption = htmltools::tags$caption(
        style = 'caption-side: top; text-align: left;',
        htmltools::tags$strong(
          paste0(subset_name, ": Correlation with SIV_positive (All variables)")
        ),
        htmltools::tags$br(),
        htmltools::tags$span(
          style = "font-size: 0.95em; color: #555;",
          "All variables (not just significant from association) in this subset are shown.",
          htmltools::tags$br(),
          "Correlation: Spearman for numeric/binary, Cramér's V for categorical (>2 levels).",
          htmltools::tags$br(),
          "FDR correction is applied to correlation p-values where available (Spearman and Cramér's V)."
        )
      ),
      filter = "top", rownames = FALSE, escape = FALSE,
      options = list(pageLength = 25, autoWidth = TRUE),
      colnames = c("Variable", "N", "Correlation", "p-Value", "p_FDR", "Test")
    )
}

# ======================== MAIN ANALYSIS =========================
outcome_var <- "SIV_positive"
exclude_vars <- make_exclude_vars(outcome_var)
husbandry_vars   <- get_husbandry_vars(exclude_vars)
animal_vars      <- get_animal_vars(exclude_vars)
environment_vars <- get_environment_vars(exclude_vars)
human_vars       <- get_human_vars(exclude_vars)

# Association analysis for each subset
results_husbandry   <- analyze_subset(husbandry_vars,   "Husbandry",   df3, outcome_var)
results_animals     <- analyze_subset(animal_vars,      "Animals",     df3, outcome_var)
results_environment <- analyze_subset(environment_vars, "Environment", df3, outcome_var)
results_human       <- analyze_subset(human_vars,       "Human",       df3, outcome_var)

# Save association table HTML for all subsets
dt_husbandry   <- make_dt(results_husbandry,   "Husbandry",   outcome_var)
dt_animals     <- make_dt(results_animals,     "Animals",     outcome_var)
dt_environment <- make_dt(results_environment, "Environment", outcome_var)
dt_human       <- make_dt(results_human,       "Human",       outcome_var)

save_html(
  tagList(
    tags$h2("Univariate Association with SIV_positive by Subset"),
    dt_husbandry, tags$hr(),
    dt_animals, tags$hr(),
    dt_environment, tags$hr(),
    dt_human
  ),
  file = "04_output/association_SIV_positive_by_subset.html"
)

# Correlation table for ALL variables in each subset (not just significant)
corr_husbandry   <- make_correlation_results(results_husbandry,   "Husbandry",   df3)
corr_animals     <- make_correlation_results(results_animals,     "Animals",     df3)
corr_environment <- make_correlation_results(results_environment, "Environment", df3)
corr_human       <- make_correlation_results(results_human,       "Human",       df3)

save_html(
  tagList(
    tags$h2("Correlation of All Variables with SIV_positive by Subset"),
    if (!is.null(corr_husbandry))   make_corr_dt(corr_husbandry,   "Husbandry")   else tags$p("No variables for Husbandry."),
    tags$hr(),
    if (!is.null(corr_animals))     make_corr_dt(corr_animals,     "Animals")     else tags$p("No variables for Animals."),
    tags$hr(),
    if (!is.null(corr_environment)) make_corr_dt(corr_environment, "Environment") else tags$p("No variables for Environment."),
    tags$hr(),
    if (!is.null(corr_human))       make_corr_dt(corr_human,       "Human")       else tags$p("No variables for Human.")
  ),
  file = "04_output/correlation_SIV_positive_by_subset.html"
)

############################################################
# Swiss Pig SIV Risk Factors: Data Preparation and PLS-DA Analysis
# ----------------------------------------------------------------
# Author: jonasalexandersteiner
#
# This script performs:
# 1. Data preparation for PLS-DA, including:
#    1. Custom binning and factor conversions (with explicit _NA handling)
#    2. Conversion of sneezing/coughing variables to logicals (with NA preserved and recoded)
#    3. Handling of missing values in numeric columns (imputation or removal)
#    4. Conversion of ALL logical and factor variables with NAs to factors with explicit "_NA" level
#    5. Identification of factors for dummy coding (excluding meta variables)
#    6. Dummy coding of multi-level factors (proper reference columns dropped, robust "_NA" handling)
#    7. Construction of the final numeric predictor matrix X (all predictors numeric and ready for modeling)
# 2. Systematic predictor filtering for modeling
# 3. Preparation of response variable for PLS-DA
# 4. Standardization of predictors and export
# 5. PLS-DA model fitting and variable importance extraction
# 6. Saving of analysis results and rendering of visualization report
############################################################

# ---- 0. Load Required Libraries ----
library(tidyverse)  # Data manipulation
library(caret)      # Dummy encoding, filtering, modeling utilities

# ---- 0a. Helper Functions ----
cut_with_na <- function(x, breaks, labels, right=FALSE) {
  y <- cut(x, breaks=breaks, labels=labels, right=right, include.lowest=TRUE)
  addNA(y, ifany=TRUE)
}

# ===============================
# 1. DATA PREPARATION STAGE
# ===============================

# 1.1. Make Working Copy of Input Data
df3_pls <- df3

# 1.2. Apply Custom Binning and Factor Conversion
df3_pls <- df3_pls %>%
  mutate(
    across(matches("_air_quality$"), as.factor),
    farrowing_piglet_litters_sneezing = ifelse(
      is.na(farrowing_piglet_litters_sneezing_percentage), NA,
      farrowing_piglet_litters_sneezing_percentage > 0
    ),
    farrowing_piglet_litters_coughing = ifelse(
      is.na(farrowing_piglet_litters_coughing_percentage), NA,
      farrowing_piglet_litters_coughing_percentage > 0
    )
  ) %>%
  mutate(
    farrowing_room_temperature = cut_with_na(
      farrowing_room_temperature, breaks = c(-Inf, 17, 20, 23, 26, Inf),
      labels = c("<17°C", "17–20°C", "20–23°C", "23–26°C", ">26°C")
    ),
    farrowing_airflow = cut_with_na(
      farrowing_airflow, breaks = c(-Inf, 0.1, 0.2, 0.4, Inf),
      labels = c("<0.1", "0.1–<0.2", "0.2–<0.4", ">=0.4")
    ),
    ai_sows_room_temperature = cut_with_na(
      ai_sows_room_temperature, breaks = c(-Inf, 15, 18, 21, 24, Inf),
      labels = c("<15", "15–<18", "18–<21", "21–<24", ">=24")
    ),
    ai_sows_airflow = cut_with_na(
      ai_sows_airflow, breaks = c(-Inf, 0.1, 0.2, 0.4, Inf),
      labels = c("<0.1", "0.1–<0.2", "0.2–<0.4", ">=0.4")
    ),
    gilts_qm_per_animal = cut_with_na(
      gilts_qm_per_animal, breaks = c(-Inf, 2.5, 3.5, 4.5, 6.0, Inf),
      labels = c("<2.5", "2.5–<3.5", "3.5–<4.5", "4.5–<6.0", ">=6.0")
    ),
    gitls_animals_per_water_source = cut_with_na(
      gitls_animals_per_water_source, breaks = c(-Inf, 4, 8, 12, 16, Inf),
      labels = c("<4", "4–<8", "8–<12", "12–<16", ">=16")
    ),
    gilts_room_temperature = cut_with_na(
      gilts_room_temperature, breaks = c(-Inf, 13, 17, 21, 25, Inf),
      labels = c("<13", "13–<17", "17–<21", "21–<25", ">=25")
    ),
    gilts_airflow = cut_with_na(
      gilts_airflow, breaks = c(-Inf, 0.1, 0.2, 0.4, Inf),
      labels = c("<0.1", "0.1–<0.2", "0.2–<0.4", ">=0.4")
    )
  ) %>%
  mutate(
    weaners_sneezing = if_else(farm_id == 5, NA_real_, weaners_sneezing),
    weaners_sneezing = cut_with_na(
      weaners_sneezing, breaks = c(-Inf, 0, 3, 10, 20, Inf),
      labels = c("0", "0–<3", "3–<10", "10–<20", "20+")
    ),
    weaners_coughing = if_else(farm_id == 5, NA_real_, weaners_coughing),
    weaners_coughing = cut_with_na(
      weaners_coughing, breaks = c(-Inf, 0, 2, 5, Inf),
      labels = c("0", "0–<2", "2–<5", "5+")
    ),
    weaners_rectal_temperature = cut_with_na(
      weaners_rectal_temperature, breaks = c(-Inf, 40.0, 40.5, 41.0, Inf),
      labels = c("<40.0", "40.0–<40.5", "40.5–<41.0", ">=41.0")
    ),
    weaners_qm_per_animal = cut_with_na(
      weaners_qm_per_animal, breaks = c(-Inf, 0.3, 0.4, 0.6, 1.0, Inf),
      labels = c("<0.3", "0.3–<0.4", "0.4–<0.6", "0.6–<1.0", ">=1.0")
    ),
    weaners_animals_per_water_source = cut_with_na(
      weaners_animals_per_water_source, breaks = c(-Inf, 10, 15, 25, 40, Inf),
      labels = c("<10", "10–<15", "15–<25", "25–<40", ">=40")
    ),
    weaners_room_temperature = cut_with_na(
      weaners_room_temperature, breaks = c(-Inf, 15, 18, 21, 25, Inf),
      labels = c("<15", "15–<18", "18–<21", "21–<25", ">=25")
    ),
    weaners_airflow = cut_with_na(
      weaners_airflow, breaks = c(-Inf, 0.1, 0.2, 0.4, Inf),
      labels = c("<0.1", "0.1–<0.2", "0.2–<0.4", ">=0.4")
    )
  ) %>%
  mutate(
    fatteners_sneezing = if_else(farm_id %in% 1:3, NA_real_, fatteners_sneezing),
    fatteners_sneezing = cut_with_na(
      fatteners_sneezing, breaks = c(-Inf, 0, 2, 5, Inf),
      labels = c("0", "0–<2", "2–<5", "5+")
    ),
    fatteners_coughing = if_else(farm_id %in% 1:3, NA_real_, fatteners_coughing),
    fatteners_coughing = cut_with_na(
      fatteners_coughing, breaks = c(-Inf, 0, 2, 5, Inf),
      labels = c("0", "0–<2", "2–<5", "5+")
    ),
    fatteners_rectal_temperature = cut_with_na(
      fatteners_rectal_temperature, breaks = c(-Inf, 40.0, 41.0, Inf),
      labels = c("<40.0", "40.0–<41.0", ">=41.0")
    ),
    fatteners_qm_per_animal = if_else(farm_id %in% 1:6, NA_real_, fatteners_qm_per_animal),
    fatteners_qm_per_animal = cut_with_na(
      fatteners_qm_per_animal, breaks = c(-Inf, 0.5, 1.0, 1.5, Inf),
      labels = c("<0.5", "0.5–<1.0", "1.0–<1.5", ">=1.5")
    ),
    fatteners_animals_per_water_source = if_else(farm_id %in% 1:6, NA_real_, fatteners_animals_per_water_source),
    fatteners_animals_per_water_source = cut_with_na(
      fatteners_animals_per_water_source, breaks = c(-Inf, 10, 15, Inf),
      labels = c("<10", "10–<15", ">=15")
    ),
    fatteners_room_temperature = if_else(farm_id %in% 1:3, NA_real_, fatteners_room_temperature),
    fatteners_room_temperature = cut_with_na(
      fatteners_room_temperature, breaks = c(-Inf, 10, 15, 20, 25, Inf),
      labels = c("10", "10–<15", "15–<20", "20–<25", ">=25")
    ),
    fatteners_airflow = if_else(farm_id %in% c(1:3,7), NA_real_, fatteners_airflow),
    fatteners_airflow = cut_with_na(
      fatteners_airflow, breaks = c(-Inf, 0.1, 0.2, 0.4, Inf),
      labels = c("<0.1", "0.1–<0.2", "0.2–<0.4", ">=0.4")
    )
  ) %>%
  dplyr::select(-farrowing_piglet_litters_sneezing_percentage, -farrowing_piglet_litters_coughing_percentage)

# 1.3. Handle Missing Values in Numeric Columns
numeric_na_to_zero <- c(
  "number_suckling_piglets", "number_weaners", "number_fattening_pigs",
  "number_young_sows", "number_old_sows", "number_boars"
)
numeric_with_na <- sapply(df3_pls, function(x) is.numeric(x) && any(is.na(x)))
numeric_names_with_na <- names(df3_pls)[numeric_with_na]
numeric_to_remove <- setdiff(numeric_names_with_na, numeric_na_to_zero)
removed_numeric_with_na <- numeric_to_remove
df3_pls <- df3_pls[!names(df3_pls) %in% numeric_to_remove]
for (col in intersect(numeric_names_with_na, numeric_na_to_zero)) {
  df3_pls[[col]][is.na(df3_pls[[col]])] <- 0
}

# 1.4. Ensure ALL logical and factor variables with NA have an explicit "_NA" level
df3_pls[] <- lapply(df3_pls, function(x) {
  if (is.logical(x)) {
    y <- as.character(x)
    y[is.na(y)] <- "_NA"
    factor(y, levels = c("FALSE", "TRUE", "_NA"))
  } else if (is.factor(x)) {
    y <- as.character(x)
    if (any(is.na(y))) {
      y[is.na(y)] <- "_NA"
      factor(y, levels = c(setdiff(levels(x), NA), "_NA"))
    } else {
      x
    }
  } else {
    x
  }
})

# ---- ENFORCE _NA LEVEL AND AT LEAST ONE "_NA" OBSERVATION FOR KEY VARIABLES ----
# This ensures dummyVars always creates the _NA dummy column for these predictors.

target_vars <- c(
  names(df3_pls)[grepl("_room_temperature$", names(df3_pls))],
  names(df3_pls)[grepl("_airflow$", names(df3_pls))],
  names(df3_pls)[grepl("_air_quality$", names(df3_pls))],
  names(df3_pls)[grepl("_qm_per_animal$", names(df3_pls))],
  names(df3_pls)[grepl("_animals_per_water_source$", names(df3_pls))],
  names(df3_pls)[grepl("_sneezing$", names(df3_pls))],
  names(df3_pls)[grepl("_coughing$", names(df3_pls))],
  names(df3_pls)[grepl("_rectal_temperature$", names(df3_pls))],
  "weaners_animals_per_feeding_site_factor",
  "fatteners_feeding_site_per_animal_factor"
)
for (v in target_vars) {
  if (v %in% names(df3_pls)) {
    # Ensure "_NA" is a level
    x <- df3_pls[[v]]
    if (!is.factor(x)) x <- factor(x)
    if (!("_NA" %in% levels(x))) levels(x) <- c(levels(x), "_NA")
    # Ensure at least one value is "_NA"
    if (!any(x == "_NA", na.rm=TRUE)) {
      x[which.max(is.na(x) | !is.na(x))] <- "_NA"  # overwrite first available slot
    }
    df3_pls[[v]] <- droplevels(x)
  }
}

# 1.5. Identify Factors for Dummy Coding (excluding meta variables)
meta_vars <- c("farm_id", "total_no_of_samples", "no_positive_pigs",
               "percentage_positive_pigs", "date_sampling", "SIV_positive")
is_dummy_factor <- sapply(df3_pls, function(x)
  is.factor(x) && !all(is.na(x)) && nlevels(x) > 1
)
dummy_factors <- setdiff(names(df3_pls)[is_dummy_factor], meta_vars)

# 1.6. Dummy Encode Multi-level Factors, Drop Reference Columns (incl. _NA reference if present)
custom_refs <- list(
  source_of_contact_grouped = "Other Private Practices",
  quarantaine_time = "other",
  production_cycle = "other"
)
if(length(dummy_factors) > 0){
  dummy_formula <- as.formula(paste("~", paste(dummy_factors, collapse="+")))
  dummy_df <- as.data.frame(
    predict(caret::dummyVars(dummy_formula, data = df3_pls, sep = "_", fullRank = TRUE), 
            newdata = df3_pls)
  )
  # Remove reference dummies as per user specification
  for (var in dummy_factors) {
    dummy_names <- grep(paste0("^", var, "_"), names(dummy_df), value = TRUE)
    levels_var <- levels(df3_pls[[var]])
    # Custom reference category, drop its dummy
    if (var %in% names(custom_refs)) {
      ref_level <- custom_refs[[var]]
      ref_dummy <- paste0(var, "_", ref_level)
      if (ref_dummy %in% dummy_names) {
        dummy_df[[ref_dummy]] <- NULL
      }
      next
    }
    # _NA reference: drop column ending "_NA"
    if ("_NA" %in% levels_var) {
      ref_dummy <- paste0(var, "_NA")
      if (ref_dummy %in% dummy_names) {
        dummy_df[[ref_dummy]] <- NULL
      }
      next
    }
    # Default: drop first level dummy
    first_dummy <- paste0(var, "_", levels_var[1])
    if (first_dummy %in% dummy_names) {
      dummy_df[[first_dummy]] <- NULL
    }
  }
} else {
  dummy_df <- NULL
}
df3_pls <- df3_pls %>%
  dplyr::select(-all_of(dummy_factors)) %>%
  dplyr::bind_cols(dummy_df)

# 1.7. Remove any columns ending in "__NA" (failsafe)
df3_pls <- df3_pls[, !grepl("__NA$", names(df3_pls)), drop=FALSE]

# 1.8. Remove reference dummies for custom categories (failsafe)
custom_ref_cols <- c(
  "source_of_contact_grouped_Other Private Practices",
  "quarantaine_time_other",
  "production_cycle_other"
)
custom_ref_cols <- custom_ref_cols[custom_ref_cols %in% names(df3_pls)]
if(length(custom_ref_cols) > 0) df3_pls <- df3_pls[, !names(df3_pls) %in% custom_ref_cols, drop = FALSE]

# 1.9. Build Final Numeric Predictor Matrix X
X <- df3_pls %>%
  dplyr::select(-all_of(meta_vars), -dplyr::starts_with("verification")) %>%
  dplyr::mutate(across(where(is.logical), as.numeric)) %>%
  dplyr::mutate(across(where(is.factor), ~ as.numeric(as.character(.)))) %>%
  dplyr::mutate(across(where(is.character), ~ as.numeric(.))) %>%
  dplyr::mutate(across(where(is.integer), as.numeric)) %>%
  as.data.frame()

DT::datatable(X)

# ===============================
# 2. SYSTEMATIC PREDICTOR FILTERING
# ===============================
# Filtering steps remove problematic predictors:
#   1. Low-variance and constant columns
#   2. Columns with all NA values
#   3. Columns with only one unique value (after NA removal)
#   4. Exact duplicate columns
#   5. Highly correlated columns (>0.98)
#   6. Aliased predictors (linear combos), groupwise by prefix
#   7. Columns removed to achieve full rank

removed_predictors_log <- data.frame(Predictor=character(), Reason=character(), DuplicateOf=character(), stringsAsFactors=FALSE)

# Helper: test if dummy_mapping exists and is not empty
is_dummy_col <- function(cols) {
  if (exists("dummy_mapping") && !is.null(dummy_mapping) && nrow(dummy_mapping) > 0) {
    cols %in% dummy_mapping$dummy_name
  } else {
    rep(FALSE, length(cols))
  }
}

# 2.1. Remove low-variance and constant columns
var_info <- data.frame(
  predictor = names(X),
  variance = sapply(X, function(x) var(x, na.rm = TRUE)),
  unique_values = sapply(X, function(x) length(unique(x)))
)
problem_idx <- which(var_info$variance < 0.001 | var_info$unique_values <= 1 | is.na(var_info$variance))
if(length(problem_idx) > 0) {
  cols_to_remove <- var_info$predictor[problem_idx]
  is_dummy <- is_dummy_col(cols_to_remove)
  if (any(is_dummy)) {
    result <- remove_dummy_group(X, removed_predictors_log, dummy_mapping, cols_to_remove[is_dummy], "Low variance / Constant / NA variance")
    X <- result$X; removed_predictors_log <- result$removed_predictors_log
    cols_to_remove <- cols_to_remove[!is_dummy]
  }
  if (length(cols_to_remove) > 0) {
    removed_predictors_log <- rbind(removed_predictors_log,
                                    data.frame(Predictor = cols_to_remove, Reason = "Low variance / Constant / NA variance", DuplicateOf = NA, stringsAsFactors = FALSE))
    X <- X[, !colnames(X) %in% cols_to_remove, drop = FALSE]
  }
}

# 2.2. Remove columns where all values are NA
all_na_cols <- colnames(X)[colSums(!is.na(X)) == 0]
if(length(all_na_cols) > 0) {
  is_dummy <- is_dummy_col(all_na_cols)
  if (any(is_dummy)) {
    result <- remove_dummy_group(X, removed_predictors_log, dummy_mapping, all_na_cols[is_dummy], "All NA values")
    X <- result$X; removed_predictors_log <- result$removed_predictors_log
    all_na_cols <- all_na_cols[!is_dummy]
  }
  if (length(all_na_cols) > 0) {
    removed_predictors_log <- rbind(removed_predictors_log,
                                    data.frame(Predictor = all_na_cols, Reason = "All NA values", DuplicateOf = NA, stringsAsFactors = FALSE))
    X <- X[, colSums(!is.na(X)) > 0, drop = FALSE]
  }
}

# 2.3. Remove columns with only one unique value (after NA removal)
constant_cols <- colnames(X)[sapply(X, function(x) length(unique(x[!is.na(x)])) <= 1)]
if(length(constant_cols) > 0) {
  is_dummy <- is_dummy_col(constant_cols)
  if (any(is_dummy)) {
    result <- remove_dummy_group(X, removed_predictors_log, dummy_mapping, constant_cols[is_dummy], "Only one unique value")
    X <- result$X; removed_predictors_log <- result$removed_predictors_log
    constant_cols <- constant_cols[!is_dummy]
  }
  if (length(constant_cols) > 0) {
    removed_predictors_log <- rbind(removed_predictors_log,
                                    data.frame(Predictor = constant_cols, Reason = "Only one unique value", DuplicateOf = NA, stringsAsFactors = FALSE))
    X <- X[, sapply(X, function(x) length(unique(x[!is.na(x)])) > 1), drop = FALSE]
  }
}

# 2.4. Impute remaining NAs with 0 (for modeling)
if (any(is.na(X))) X[is.na(X)] <- 0

# 2.5. Remove exact duplicate columns (keep only the first occurrence)
dup_cols_idx <- which(duplicated(t(X)))
dup_cols <- colnames(X)[dup_cols_idx]
if(length(dup_cols) > 0) {
  for (i in seq_along(dup_cols_idx)) {
    idx <- dup_cols_idx[i]
    dup_name <- colnames(X)[idx]
    orig_idx <- which(apply(t(X)[1:(idx-1),,drop=FALSE], 1, function(row) all(row == X[,idx])))
    orig_name <- if (length(orig_idx) > 0) colnames(X)[orig_idx[1]] else NA
    removed_predictors_log <- rbind(
      removed_predictors_log,
      data.frame(
        Predictor = dup_name, 
        Reason = "Duplicate predictor", 
        DuplicateOf = orig_name,
        stringsAsFactors = FALSE
      )
    )
  }
  X <- X[, !duplicated(t(X)), drop=FALSE]
}

# 2.6. Remove highly correlated columns (>0.98 correlation)
cor_matrix <- cor(X)
if (any(is.na(cor_matrix))) {
  badcols <- unique(c(which(rowSums(is.na(cor_matrix)) > 0), which(colSums(is.na(cor_matrix)) > 0)))
  if (length(badcols) > 0) {
    cols_to_remove <- colnames(X)[badcols]
    is_dummy <- is_dummy_col(cols_to_remove)
    if (any(is_dummy)) {
      result <- remove_dummy_group(X, removed_predictors_log, dummy_mapping, cols_to_remove[is_dummy], "NA in correlation matrix (likely zero variance)")
      X <- result$X; removed_predictors_log <- result$removed_predictors_log
      cols_to_remove <- cols_to_remove[!is_dummy]
    }
    if (length(cols_to_remove) > 0) {
      removed_predictors_log <- rbind(removed_predictors_log,
                                      data.frame(Predictor = cols_to_remove, Reason = "NA in correlation matrix (likely zero variance)", DuplicateOf = NA, stringsAsFactors = FALSE))
      X <- X[, -badcols, drop = FALSE]
      cor_matrix <- cor(X)
    }
  }
}
highly_correlated <- findCorrelation(cor_matrix, cutoff = 0.98)
if (length(highly_correlated) > 0) {
  cols_to_remove <- colnames(X)[highly_correlated]
  is_dummy <- is_dummy_col(cols_to_remove)
  if (any(is_dummy)) {
    result <- remove_dummy_group(X, removed_predictors_log, dummy_mapping, cols_to_remove[is_dummy], "High correlation (>0.98)")
    X <- result$X; removed_predictors_log <- result$removed_predictors_log
    cols_to_remove <- cols_to_remove[!is_dummy]
  }
  if (length(cols_to_remove) > 0) {
    removed_predictors_log <- rbind(removed_predictors_log,
                                    data.frame(Predictor = cols_to_remove, Reason = "High correlation (>0.98)", DuplicateOf = NA, stringsAsFactors = FALSE))
    X <- X[, !colnames(X) %in% cols_to_remove, drop = FALSE]
  }
}

# 2.7. Remove aliased (linear combo) predictors, groupwise for all dummies with the same prefix
lin_combo <- caret::findLinearCombos(X)
if(!is.null(lin_combo$remove) && length(lin_combo$remove) > 0) {
  cols_to_remove <- colnames(X)[lin_combo$remove]
  if (length(cols_to_remove) > 0) {
    # Get prefix up to last underscore for each flagged dummy column
    get_prefix <- function(x) sub("^(.*_)[^_]+$", "\\1", x)
    prefixes <- unique(get_prefix(cols_to_remove))
    # Remove all columns with these prefixes (i.e. all dummies for this categorical variable)
    groupwise_cols_to_remove <- unique(unlist(lapply(prefixes, function(pfx) {
      grep(paste0("^", pfx), colnames(X), value = TRUE)
    })))
    removed_predictors_log <- rbind(
      removed_predictors_log,
      data.frame(
        Predictor = groupwise_cols_to_remove,
        Reason = "Linear combination (aliased, groupwise removed by prefix)",
        DuplicateOf = NA,
        stringsAsFactors = FALSE
      )
    )
    X <- X[, !colnames(X) %in% groupwise_cols_to_remove, drop = FALSE]
  }
}

# 2.8. Remove any remaining columns to achieve full rank (QR decomposition)
X_matrix <- as.matrix(X)
qr_X <- qr(X_matrix)
if (qr_X$rank < ncol(X_matrix)) {
  keep_cols <- qr_X$pivot[seq_len(qr_X$rank)]
  remove_cols <- setdiff(seq_len(ncol(X_matrix)), keep_cols)
  if (length(remove_cols) > 0) {
    cols_to_remove <- colnames(X_matrix)[remove_cols]
    is_dummy <- is_dummy_col(cols_to_remove)
    if (any(is_dummy)) {
      result <- remove_dummy_group(X, removed_predictors_log, dummy_mapping, cols_to_remove[is_dummy], "Removed to achieve full rank (QR)")
      X <- result$X; removed_predictors_log <- result$removed_predictors_log
      cols_to_remove <- cols_to_remove[!is_dummy]
    }
    if (length(cols_to_remove) > 0) {
      removed_predictors_log <- rbind(removed_predictors_log,
                                      data.frame(Predictor = cols_to_remove, Reason = "Removed to achieve full rank (QR)", DuplicateOf = NA, stringsAsFactors = FALSE))
      X <- X[, keep_cols, drop = FALSE]
    }
  }
}

write.csv(removed_predictors_log, "removed_predictors_log.csv", row.names = FALSE)

# ===============================
# 3. PREPARE RESPONSE VARIABLE
# ===============================
# Converts SIV_positive to a binary factor with TRUE/FALSE levels, handles various encodings
Y_raw <- df3_pls$SIV_positive
if (!is.factor(Y_raw)) Y_raw <- as.factor(Y_raw)
Y <- factor(
  ifelse(
    Y_raw %in% c(1, "1", TRUE, "TRUE", "positive"), TRUE,
    ifelse(Y_raw %in% c(0, "0", FALSE, "FALSE", "negative"), FALSE, NA)
  ),
  levels = c(FALSE, TRUE)
)
if (!is.factor(Y)) stop("SIV_positive could not be coerced to a factor for PLS-DA.")
if (length(levels(Y)) != 2) stop("SIV_positive must have exactly two levels (TRUE/FALSE) for binary PLS-DA.")
if (any(is.na(Y))) {
  valid_idx <- !is.na(Y)
  X <- X[valid_idx, , drop = FALSE]
  Y <- Y[valid_idx]
}

# ===============================
# 4. STANDARDIZE PREDICTORS AND EXPORT DATA
# ===============================
# - Standardizes X for modeling and exports predictors and response for reproducibility
X <- as.data.frame(scale(X))
write.csv(X, "model_X_used.csv", row.names = TRUE)
write.csv(data.frame(SIV_positive = Y), "model_Y_used.csv", row.names = TRUE)

# ===============================
# 5. PLS-DA MODEL FITTING AND RESULT EXTRACTION
# ===============================
# - Fits the PLS-DA model, extracts variable importance, scores, loadings, explained variance, and saves results

max_comp <- min(3, ncol(X), floor(nrow(X)/4))
set.seed(123)
plsda_model <- plsda(X, Y, ncomp = max_comp)
ncomp_optimal <- tryCatch({
  cv <- perf(plsda_model, validation = "Mfold", folds = 5, progressBar = FALSE)
  if (is.matrix(cv$error.rate$overall)) {
    error_rate_vec <- colMeans(cv$error.rate$overall)
  } else {
    error_rate_vec <- cv$error.rate$overall
  }
  rmsep_values <- sqrt(error_rate_vec)
  which.min(rmsep_values)
}, error = function(e) 2)
ncomp_model <- plsda_model$ncomp
if (is.na(ncomp_optimal) || ncomp_optimal < 1) ncomp_optimal <- 1
if (ncomp_optimal > ncomp_model) ncomp_optimal <- ncomp_model

if (inherits(plsda_model, "plsda")) {
  loadings <- plsda_model$loadings
  explained_var <- plsda_model$Xvar / plsda_model$Xtotvar
  vip_manual <- apply(loadings, 1, function(load) sum(abs(load) * explained_var))
  coef_df <- data.frame(Predictor = rownames(loadings), VIP = vip_manual) %>% arrange(desc(VIP))
} else {
  stop("The model type is incompatible with VIP score extraction.")
}
loadings_df <- as.data.frame(plsda_model$loadings[, 1:plsda_model$ncomp, drop = FALSE])
scores_df <- as.data.frame(plsda_model$scores[, 1:plsda_model$ncomp, drop = FALSE])
colnames(scores_df) <- paste0("Comp ", 1:ncol(scores_df))
explained_var <- plsda_model$Xvar / plsda_model$Xtotvar
names(explained_var) <- paste0("Comp ", seq_along(explained_var))

# ===============================
# 6. SAVE ANALYSIS RESULTS AND RENDER REPORT
# ===============================
# - Saves all key results and renders an R Markdown visualization report

X_mat <- as.matrix(X)
if (is.null(rownames(X_mat)) || !is.character(rownames(X_mat))) rownames(X_mat) <- paste0("Sample", seq_len(nrow(X_mat)))
rownames(X_mat) <- make.unique(as.character(rownames(X_mat)))
if (is.null(colnames(X_mat)) || !is.character(colnames(X_mat))) colnames(X_mat) <- paste0("Var", seq_len(ncol(X_mat)))
colnames(X_mat) <- make.unique(as.character(colnames(X_mat)))

SIV_positive <- factor(ifelse(Y == TRUE, "positive", "negative"), levels = c("negative", "positive"))
output_dir <- "04_output/SIV_PLS_Analysis_Results"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
results <- list(
  plsda_model = plsda_model,
  coef_df = coef_df,
  loadings_df = loadings_df,
  scores_df = scores_df,
  explained_var = explained_var,
  ncomp_optimal = ncomp_optimal,
  sample_size = nrow(X_mat),
  predictor_count = ncol(X_mat),
  original_predictor_count = ncol(df3_pls) - length(meta_vars) - 1,
  date_time = Sys.time(),
  author = "jonasalexandersteiner",
  X = X_mat,
  SIV_positive = SIV_positive
)
saveRDS(results, file.path(output_dir, "plsda_analysis_results.rds"))

# Render R Markdown Visualization Report
rmd_file <- file.path(output_dir, "SIV_PLS_visualization.Rmd")
html_file <- file.path(output_dir, "SIV_BinaryPLSR_Analysis_Report.html")
if (!file.exists(rmd_file)) stop(paste("R Markdown file does not exist:", rmd_file))
rmarkdown::render(
  input = rmd_file,
  output_file = html_file,
  output_dir = output_dir,
  envir = new.env(parent = globalenv())
)

############################################################
# End of SIV Binary PLSR (PLS-DA) Analysis Script
############################################################