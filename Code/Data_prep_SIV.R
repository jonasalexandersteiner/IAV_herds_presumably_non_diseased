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
Sys.setenv(LANG = "en")        # Ensure English setup
Sys.setlocale("LC_TIME", "C") 
options(scipen = 999)          # Avoid scientific notation for numerics
rm(list = ls())                # Remove all objects from workspace for a clean start


# ---- 2. Library Management ----
library(pacman)
library(dplyr)
library(ggplot2)
library(scales)
# Load all required packages (will install if missing)
pacman::p_load(
  tidyverse, haven, readxl, writexl, janitor, lubridate, DT,
  broom, purrr, stringr, knitr, kableExtra ,deeplr
)

# ---- 3. Directory Preparation ----
d_proj <- "C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/SIV_Projekt/Data_processing/SIV_farms100"
setwd(d_proj)
dirs <- file.path(d_proj, c("01_oridata", "02_data", "03_code", "04_output"))
# Create directories if they do not exist (recursive for nested folders)
walk(dirs, ~ if(!dir.exists(.x)) dir.create(.x, recursive = TRUE))
d_oridata <- dirs[1]

# ---- 4. Data Loading and Initial Cleaning ----
# Read main Excel file, treat "NA" as missing, clean column names to snake_case
sivfarms <- read_excel(file.path(d_oridata, "SIV_farms.xlsx"), na = "NA") %>%
  clean_names()

# Keep ONLY asymptomatic farms in df1
# Assumes symptomatic_report is already logical; if not, use the robust mapping shown below.
df1 <- sivfarms %>%
  dplyr::filter(symptomatic_report == FALSE) %>%
  dplyr::select(-symptomatic_report)

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
  txt <- trimws(as.character(x))
  
  # Extract all numbers present in the text
  nums <- as.numeric(stringr::str_extract_all(txt, "\\d+")[[1]])
  if (length(nums) == 0) return(NA_character_)
  
  # Any multi-age indication (commas, semicolons, hyphen, en-dash) → mixed weaners
  if (length(nums) > 1 || grepl("[,;\\-–]", txt)) {
    return("weaners_mixed_ages")
  }
  
  n <- nums[1]
  if (n >= 4 && n <= 5)   return("weaners_4–5")
  if (n >= 6 && n <= 7)   return("weaners_6–7")
  if (n >= 8 && n <= 9)   return("weaners_8–9")
  if (n >= 10 && n <= 12) return("weaners_10–12")
  return(NA_character_)
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

# --- 7.1 Import and join rectal temperature and sampling summary
sample_pigs_path <- "C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/SIV_Projekt/Data_processing/SIV_farms100/01_oridata/Samples_pigs_qPCR.xlsx"
df_sample_pigs <- read_excel(sample_pigs_path)

df_temp_summary <- df_sample_pigs %>%
  filter(
    !(Sample_AID %in% c(as.character(40:44), as.character(610:628))) #remove samples were double examination of farms that were excluded
  ) %>%
  mutate(
    Rectal_temperature = as.numeric(Rectal_temperature),
    Ct_value = as.numeric(Ct_value)
  ) %>%
  group_by(Farm_ID) %>%
  dplyr::summarize(
    # Rectal temperature
    rectal_temperature_max = if (all(is.na(Rectal_temperature))) NA_real_ else round(max(Rectal_temperature, na.rm = TRUE), 1),
    rectal_temperature_avg = if (all(is.na(Rectal_temperature))) NA_real_ else round(mean(Rectal_temperature, na.rm = TRUE), 1),
    # Total samples
    total_samples = n_distinct(Sample_AID),
    # Positive pigs and percentage
    positive_pigs = sum(qPCR_result == "positive", na.rm = TRUE),
    percent_positive_pigs = ifelse(total_samples > 0, positive_pigs / total_samples * 100, NA_real_),
    # Ct value statistics (only for positive pigs)
    min_ct = if (any(qPCR_result == "positive" & !is.na(Ct_value))) min(Ct_value[qPCR_result == "positive"], na.rm = TRUE) else NA_real_,
    max_ct = if (any(qPCR_result == "positive" & !is.na(Ct_value))) max(Ct_value[qPCR_result == "positive"], na.rm = TRUE) else NA_real_,
    mean_ct = if (any(qPCR_result == "positive" & !is.na(Ct_value))) mean(Ct_value[qPCR_result == "positive"], na.rm = TRUE) else NA_real_,
    sd_ct = if (any(qPCR_result == "positive" & !is.na(Ct_value))) sd(Ct_value[qPCR_result == "positive"], na.rm = TRUE) else NA_real_
  ) %>%
  ungroup()

df2 <- df1 %>%
  left_join(df_temp_summary, by = c("farm_id" = "Farm_ID")) %>%
  relocate(
    rectal_temperature_max, rectal_temperature_avg, 
    total_samples, positive_pigs, percent_positive_pigs, 
    min_ct, max_ct, mean_ct, sd_ct,
    .after = farm_id
  ) %>%
  
  
  # 7.2 Factor conversions (explicit for categorical variables)
  mutate(
    canton = as.factor(canton),
    production_type = as.factor(production_type),
    sgd_qgs = as.factor(sgd_qgs),
    mode_stable_occupation_ai_centre = as.factor(mode_stable_occupation_ai_centre),
    mode_stable_occupation_gestation_stable = as.factor(mode_stable_occupation_gestation_stable),
    mode_stable_occupation_farrowing_stable = as.factor(mode_stable_occupation_farrowing_stable),
    cross_fostering_farrowing_stable = as.factor(cross_fostering_farrowing_stable),
    mode_stable_occupation_weaner_stable = as.factor(mode_stable_occupation_weaner_stable),
    mode_stable_occupation_fattening_stable = as.factor(mode_stable_occupation_fattening_stable),
    isolation_respiratory_diseased_pigs = as.factor(isolation_respiratory_diseased_pigs),
    cleaning_ai_centre = as.factor(cleaning_ai_centre),
    cleaning_gestation_stable = as.factor(cleaning_gestation_stable),
    cleaning_farrowing_stable = as.factor(cleaning_farrowing_stable),
    cleaning_weaner_stable = as.factor(cleaning_weaner_stable),
    cleaning_fattening_stable = as.factor(cleaning_fattening_stable),
    cleaning_quarantine = as.factor(cleaning_quarantine),
    disinfection_ai_centre = as.factor(disinfection_ai_centre),
    disinfection_gestation_stable = as.factor(disinfection_gestation_stable),
    disinfection_farrowing_stable = as.factor(disinfection_farrowing_stable),
    disinfection_weaner_stable = as.factor(disinfection_weaner_stable),
    disinfection_fattening_stable = as.factor(disinfection_fattening_stable),
    disinfection_quarantine = as.factor(disinfection_quarantine),
    drying_ai_centre = as.factor(drying_ai_centre),
    drying_gestation_stable = as.factor(drying_gestation_stable),
    drying_farrowing_stable = as.factor(drying_farrowing_stable),
    drying_weaner_stable = as.factor(drying_weaner_stable),
    drying_fattening_stable = as.factor(drying_fattening_stable),
    drying_quarantine = as.factor(drying_quarantine),
    cleaning_disinfection_transport_vehicle = as.factor(cleaning_disinfection_transport_vehicle),
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
    average_daily_weight_gain_weaners = as.factor(average_daily_weight_gain_weaners),
    average_daily_weight_gain_fattening_pigs = as.factor(average_daily_weight_gain_fattening_pigs),
    feed_conversion_rate_weaners = as.factor(feed_conversion_rate_weaners),
    feed_conversion_rate_fattening_pigs = as.factor(feed_conversion_rate_fattening_pigs),
    respiratory_history_swine = as.factor(respiratory_history_swine),
    time_respiratory_disease = as.factor(time_respiratory_disease),
    visitors_in_stable = as.factor(visitors_in_stable),
    symptom_severity = as.factor(symptom_severity),
    visitors_cumulative_contact_hours = as.factor(visitors_cumulative_contact_hours),
    ppe_visitors = as.factor(ppe_visitors),
    separation_between_production_units = as.factor(separation_between_production_units),
    separation_within_production_units = as.factor(separation_within_production_units),
    separation_quarantine_area = as.factor(separation_quarantine_area),
    bird_nests = as.factor(bird_nests),
    verification_outside_area_contact_poultry = as.factor(verification_outside_area_contact_poultry),
    verification_outside_area_contact_wild_birds = as.factor(verification_outside_area_contact_wild_birds),
    verification_contact_poultry_stable = as.factor(verification_contact_poultry_stable),
    verification_isolation_respiratory_diseased_pigs = as.factor(verification_isolation_respiratory_diseased_pigs),
    farrowing_air_quality = as.factor(farrowing_air_quality),
    farrowing_airspace_with_other_agegroup = as.factor(farrowing_airspace_with_other_agegroup),
    ai_sows_air_quality = as.factor(ai_sows_air_quality),
    ai_airspace_with_other_agegroup = as.factor(ai_airspace_with_other_agegroup),
    gestation_sows_reduced_general_wellbeing = as.factor(gestation_sows_reduced_general_wellbeing),
    gestation_sows_air_quality = as.factor(gestation_sows_air_quality),
    gestation_sows_airspace_with_other_agegroup = as.factor(gestation_sows_airspace_with_other_agegroup),
    weaners_air_quality = as.factor(weaners_air_quality),
    weaners_airspace_with_other_agegroup = as.factor(weaners_airspace_with_other_agegroup),
    fattening_pigs_air_quality = as.factor(fattening_pigs_air_quality),
    fattening_pigs_airspace_with_other_agegroup = as.factor(fattening_pigs_airspace_with_other_agegroup),
    caretaker_contact_other_pigs = as.factor(caretaker_contact_other_pigs),
    caretaker_contact_poultry = as.factor(caretaker_contact_poultry),
    production_cycle = factor(
      case_when(
        production_cycle %in% c(3,2,1,6,5,4,7) ~ as.character(production_cycle),
        TRUE ~ "other"
      ),
      levels = c("3","2","1","6","5","4","7","other")
    ),
    quarantine_time = factor(
      case_when(
        quarantine_time %in% c(16,4,3,6,5,20,2) ~ as.character(quarantine_time),
        !is.na(quarantine_time) ~ "other"
        
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
      levels = c("weaners_4–5", "weaners_6–7", "weaners_8–9", "weaners_10–12", "weaners_mixed_ages")
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
    fattening_pigs_feeding_site_per_animal_factor = factor(
      sapply(fattening_pigs_feeding_site_width_per_animal, make_category),
      levels = category_levels
    )
  ) %>%
  # 7.3 Logical conversions (robust, explicit)
  mutate(
    Farrowing_on_farm   = as.logical(ifelse(number_suckling_piglets > 0, TRUE, FALSE)),
    Isemination_on_farm = as.logical(ifelse(number_boars > 0, TRUE, FALSE)),
    Gestation_on_farm   = as.logical(ifelse(number_boars > 0, TRUE, FALSE)),
    Weaners_on_farm     = as.logical(ifelse(number_weaners > 0, TRUE, FALSE)),
    Fattening_on_farm   = as.logical(ifelse(number_fattening_pigs > 0, TRUE, FALSE)),
    outside_area_contact_poultry = ifelse(outside_area_contact_poultry == 1, FALSE, TRUE),
    outside_area_contact_wild_birds = ifelse(outside_area_contact_wild_birds == 1, FALSE, TRUE),
    contact_bird_in_stable = ifelse(contact_bird_in_stable == 1, FALSE, TRUE),
    caretaker_hands_washed_before_entry = as.logical(caretaker_hands_washed_before_entry),
    respiratory_history_contact_person = ifelse(is.na(respiratory_history_contact_person), FALSE, TRUE),
    caretaker_boot_disinfection = ifelse(caretaker_boot_disinfection == 1, FALSE, TRUE),
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
    symptom_human_bronchitis       = make_logical(grepl("\\b3\\b", symptom_type_human)),
    symptom_human_pneumonia        = make_logical(grepl("\\b4\\b", symptom_type_human)),
    symptom_human_fever            = make_logical(grepl("\\b5\\b", symptom_type_human)),
    symptom_human_headache         = make_logical(grepl("\\b6\\b", symptom_type_human)),
    symptom_human_myalgia          = make_logical(grepl("\\b7\\b", symptom_type_human)),
    bird_nests = ifelse(bird_nests == 1, FALSE, TRUE),
    chronic_disease_condition = ifelse(
      is.na(chronic_disease_condition) | chronic_disease_condition == "",
      FALSE,
      TRUE
    ),
    farrowing_nest_temperature = ifelse(
      is.na(farrowing_nest_temperature),
      NA,
      farrowing_nest_temperature == "acceptable"
    ),
    IAV_positive = positive_pigs > 0
  ) %>%
  # 7.4 Integer/Numeric conversions
  mutate(
    caretaker_number = as.integer(as.character(caretaker_number)),
    gestation_sows_room_temperature = as.numeric(gestation_sows_room_temperature),
    farrowing_room_temperature = as.numeric(farrowing_room_temperature),
    visitors_in_stable_recent = as.integer(visitors_in_stable_recent),
    visitors_cumulative_contact_hours = as.integer(visitors_cumulative_contact_hours),
    across(
      intersect(
        c(
          "farm_id", "total_samples", "positive_pigs", "herdsize",
          "number_suckling_piglets", "number_weaners", "number_fattening_pigs", "number_young_sows",
          "number_old_sows", "number_boars",
          "number_of_origins", "number_of_origins_suckling_piglets", "number_of_origins_weaners", "number_of_origins_fattening_pigs",
          "number_of_origins_young_sows", "number_of_origins_old_sows", "number_of_origins_boars",
          "start_time_current_outbreak", "starting_point_current_disease"
        ), names(.)
      ),
      ~as.integer(.)
    ),
    across(
      intersect(
        c(
          "percent_positive_pigs", "min_ct", "max_ct", "mean_ct", "sd_ct",
          "farrowing_piglets_sneezing", "farrowing_piglets_coughing",
          "farrowing_room_temperature", "farrowing_airflow",
          "ai_sows_room_temperature", "ai_sows_airflow", "gestation_sows_qm_per_animal", "gestation_sows_animals_per_water_source",
          "gestation_sows_room_temperature", "gestation_sows_airflow", "weaners_sneezing", "weaners_coughing",
          "weaners_qm_per_animal", "weaners_animals_per_water_source", "weaners_room_temperature", "weaners_airflow",
          "fattening_pigs_sneezing", "fattening_pigs_coughing", "fattening_pigs_qm_per_animal",
          "fattening_pigs_animals_per_water_source", "fattening_pigs_room_temperature", "fattening_pigs_airflow",
          "percent_diseased_suckling_piglets", "percent_diseased_weaners", "percent_diseased_fattening_pigs",
          "percent_diseased_young_sows", "percent_diseased_old_sows", "percent_diseased_boars",
          "visitors_cumulative_contact_hours", "weaners_reduced_general_wellbeing", "weaners_discharge",
          "fattening_pigs_reduced_general_wellbeing", "fattening_pigs_discharge", "fattening_pigs_room_temperature"
        ), names(.)
      ),
      as.numeric
    )
  ) %>%
  # 7.5 Renaming and relocating columns for logical grouping
  rename(
    farrowing_nest_temperature_ok = farrowing_nest_temperature
  ) %>%
  relocate(
    IAV_positive, .after = farm_id
  ) %>%
  relocate(
    total_samples, positive_pigs, percent_positive_pigs,
    min_ct, max_ct, mean_ct, sd_ct,
    .after = IAV_positive
  ) %>%
  relocate(
    date_sampling, canton, herdsize, production_type,
    Farrowing_on_farm, Isemination_on_farm, Gestation_on_farm, Weaners_on_farm, Fattening_on_farm,
    .after = sd_ct
  ) %>%
  relocate(
    weaners_animals_per_feeding_site, .before = weaners_animals_per_water_source
  ) %>%
  relocate(
    weaners_animals_per_feeding_site_factor, .after = weaners_animals_per_feeding_site
  ) %>%
  relocate(
    fattening_pigs_feeding_site_width_per_animal, .before = fattening_pigs_animals_per_water_source
  ) %>%
  relocate(
    fattening_pigs_feeding_site_per_animal_factor, .after = fattening_pigs_feeding_site_width_per_animal
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
    symptom_human_sneezing, symptom_human_coughing, symptom_human_bronchitis,
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

# ---- 8. Save cleaned data to RDS ----
#Uncomment to save data after validation
#saveRDS(df2, file.path(dirs[2], "siv_farms_cleaned.rds"))


# ---- 9. Quick Data Overview ----
# Use these for further inspection and QA
#glimpse(df2)
#DT::datatable(df2)
#utils::View(df2)
#sapply(df2, class)
#rmarkdown::render("eda_report.Rmd", 
#output_file = "eda_report.html", 
#output_dir = "04_output", 
#envir = globalenv())


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
# Project: SIV_Projekt - SIV farms data processing and reporting
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
     
    #create respiratory_signs
     respiratory_signs = case_when(
       farm_id %in% c(5, 7,71,72,73) ~ TRUE,  # Manual override for outbreak farms: 5,7 piglet outbreak, 71/72/73 external observer no CI/SI
       if_all(
         c("weaners_coughing", "fattening_pigs_coughing",
           "rectal_temperature_max",
           "weaners_sneezing", "fattening_pigs_sneezing",
           "weaners_reduced_general_wellbeing", "fattening_pigs_reduced_general_wellbeing"),
         is.na
       ) ~ NA,
       TRUE ~ (
         coalesce(weaners_coughing >= 2.5, FALSE) |
           coalesce(fattening_pigs_coughing >= 2.5, FALSE) |
           (
             coalesce(rectal_temperature_max >= 40.5, FALSE) &
               (
                 coalesce(weaners_sneezing >= 20, FALSE) |
                   coalesce(fattening_pigs_sneezing >= 20, FALSE) |
                   coalesce(weaners_reduced_general_wellbeing, FALSE) |
                   coalesce(fattening_pigs_reduced_general_wellbeing, FALSE)
               )
           )
       )
     ),
     
    # Format date_sampling as "MON YYYY" (e.g. "JAN 2024")
    date_sampling = paste(toupper(format(date_sampling, "%b")), format(date_sampling, "%Y")),        
    
    
    
    # 5d. Safe pattern matching function (error-proof)
      
      # Create season_sampling as a factor with 4 levels based on month in date_sampling
      season_sampling = factor(
        case_when(
          str_detect(date_sampling, "DEC|JAN|FEB") ~ "winter",
          str_detect(date_sampling, "MAR|APR|MAY") ~ "spring",
          str_detect(date_sampling, "JUN|JUL|AUG") ~ "summer",
          str_detect(date_sampling, "SEP|OCT|NOV") ~ "autumn",
          TRUE ~ NA_character_
        ),
        levels = c("winter", "spring", "summer", "autumn")
      ),
    
    
    
    # Canton_factor: aggregate Swiss cantons into study regions to ensure sufficient observations per level
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
    
    # Production_type_factor: group production types into 5 categories to ensure sufficient observations per level
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
    
    # Simplify production_cycle into factor levels 1/2/3 or 'other' to ensure sufficient observations per level
    production_cycle = factor(
      case_when(
        production_cycle %in% c(1, 2, 3) ~ as.character(production_cycle),
        !is.na(production_cycle) ~ "other",
        TRUE ~ NA_character_
      ),
      levels = c("1", "2", "3", "other")
    ),
    
    # Transform occupancy and cleaning variables to binary/factor to ensure sufficient observations per level 
    mode_stable_occupation_ai_centre = case_when(
      is.na(mode_stable_occupation_ai_centre) ~ NA,
      mode_stable_occupation_ai_centre == 1 ~ TRUE,
      TRUE ~ FALSE
    ),
    mode_stable_occupation_gestation_stable = case_when(
      is.na(mode_stable_occupation_gestation_stable) ~ NA,
      mode_stable_occupation_gestation_stable == 6 ~ TRUE,
      TRUE ~ FALSE
    ),
    mode_stable_occupation_farrowing_stable = case_when(
      is.na(mode_stable_occupation_farrowing_stable) ~ NA,
      mode_stable_occupation_farrowing_stable == 1 ~ TRUE,
      TRUE ~ FALSE
    ),
    cross_fostering_farrowing_stable = factor(case_when(
      is.na(cross_fostering_farrowing_stable) ~ NA_character_,
      cross_fostering_farrowing_stable %in% c(1,2,3) ~ "1",
      cross_fostering_farrowing_stable == 4 ~ "2",
      cross_fostering_farrowing_stable == 5 ~ "3",
    ), levels = c("1","2","3")),
    mode_stable_occupation_weaner_stable = case_when(
      is.na(mode_stable_occupation_weaner_stable) ~ NA,
      mode_stable_occupation_weaner_stable == 1 ~ TRUE,
      TRUE ~ FALSE
    ),
    mode_stable_occupation_fattening_stable = case_when(
      is.na(mode_stable_occupation_fattening_stable) ~ NA,
      mode_stable_occupation_fattening_stable == 1 ~ TRUE,
      TRUE ~ FALSE
    ),
    outside_area = if_any(starts_with("outside_area"), ~.x == TRUE),
    cleaning_ai_centre = factor(case_when(
      is.na(cleaning_ai_centre) ~ NA_character_,
      cleaning_ai_centre == 2 ~ "1",
      cleaning_ai_centre == 4 ~ "2",
      cleaning_ai_centre == 5 ~ "3"
    ), levels = c("1","2","3")),
    cleaning_gestation_stable = case_when(
      is.na(cleaning_gestation_stable) ~ NA,
      cleaning_gestation_stable == 2 ~ FALSE,
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
    cleaning_fattening_stable = factor(case_when(
      is.na(cleaning_fattening_stable) | cleaning_fattening_stable == 1 ~ NA_character_,
      cleaning_fattening_stable == 2 ~ "1",
      cleaning_fattening_stable %in% c(3,4) ~ "2",
      cleaning_fattening_stable %in% c(5) ~ "3"
    ), levels = c("1","2","3")),
    cleaning_quarantine = factor(case_when(
      is.na(cleaning_quarantine) | cleaning_quarantine == 1 ~ NA_character_,
      cleaning_quarantine == 2 ~ "1",
      cleaning_quarantine == 4 ~ "2",
      cleaning_quarantine == 5 ~ "3"
    ), levels = c("1","2","3")),
    disinfection_ai_centre = case_when(
      is.na(disinfection_ai_centre) ~ NA,
      disinfection_ai_centre == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    disinfection_gestation_stable = case_when(
      is.na(disinfection_gestation_stable) ~ NA,
      disinfection_gestation_stable == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    disinfection_farrowing_stable = factor(case_when(
      is.na(disinfection_farrowing_stable) ~ NA_character_,
      disinfection_farrowing_stable == 2 ~ "1",
      disinfection_farrowing_stable == 4 ~ "2",
      disinfection_farrowing_stable == 5 ~ "3"
    ), levels = c("1", "2", "3")),
    disinfection_weaner_stable = factor(case_when(
      is.na(disinfection_weaner_stable) ~ NA_character_,
      disinfection_weaner_stable == 2 ~ "1",
      disinfection_weaner_stable %in% c(3,4) ~ "2",
      disinfection_weaner_stable == 5 ~ "3"
    ), levels = c("1", "2", "3")),
    disinfection_fattening_stable = case_when(
      is.na(disinfection_fattening_stable) ~ NA,
      disinfection_fattening_stable == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    disinfection_quarantine = case_when(
      is.na(disinfection_quarantine) | disinfection_quarantine == 1 ~ NA,
      disinfection_quarantine == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    drying_ai_centre = factor(case_when(
      is.na(drying_ai_centre) ~ NA_character_,
      drying_ai_centre == 2 ~ "1",
      drying_ai_centre == 4 ~ "2",
      drying_ai_centre == 5 ~ "3"
    ), levels = c("1", "2", "3")),
    drying_gestation_stable = case_when(
      is.na(drying_gestation_stable) ~ NA,
      drying_gestation_stable == 2 ~ FALSE,
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
    drying_fattening_stable = factor(case_when(
      is.na(drying_fattening_stable) | drying_fattening_stable == 1 ~ NA_character_,
      drying_fattening_stable == 2 ~ "1",
      drying_fattening_stable %in% c(3,4) ~ "2",
      drying_fattening_stable == 5 ~ "3"
    ), levels = c("1", "2", "3")),
    drying_quarantine = case_when(
      is.na(drying_quarantine) | drying_quarantine == 1 ~ NA,
      drying_quarantine == 2 ~ FALSE,
      TRUE ~ TRUE
    ),
    cleaning_disinfection_transport_vehicle = case_when(
      is.na(cleaning_disinfection_transport_vehicle) | cleaning_disinfection_transport_vehicle == 1 ~ NA,
      cleaning_disinfection_transport_vehicle == 5 ~ TRUE,
      cleaning_disinfection_transport_vehicle %in% c(2,4) ~ FALSE
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
    feed_conversion_rate_fattening_pigs = case_when(
      is.na(feed_conversion_rate_fattening_pigs) ~ NA,
      feed_conversion_rate_fattening_pigs == 1 ~ TRUE,
      feed_conversion_rate_fattening_pigs == 2 ~ FALSE
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
    fattening_pigs_diseased = percent_diseased_fattening_pigs != 0,
    young_sows_diseased = percent_diseased_young_sows != 0,
    old_sows_diseased = percent_diseased_old_sows != 0,
    boars_diseased = percent_diseased_boars != 0,
    report_killed_suckling_piglets = percent_killed_suckling_piglets != 0,
    report_killed_weaners = percent_killed_weaners != 0,
    report_killed_fattening_pigs = percent_killed_fattening_pigs != 0,
    report_killed_young_sows = percent_killed_young_sows != 0,
    report_killed_old_sows = percent_killed_old_sows != 0,
    starting_point_current_disease = na_if(starting_point_current_disease, 0),
    symptom_severity = case_when(
      is.na(symptom_severity) ~ NA,
      symptom_severity == 1 ~ FALSE,
      symptom_severity %in% c(2,3) ~ TRUE
    ),
    separation_between_production_units = factor(
      case_when(
        separation_between_production_units == 2 ~ "1",
        separation_between_production_units == 3 ~ "2",
        separation_between_production_units %in% c(1, 4) ~ "3",
        TRUE ~ NA_character_
      ),
      levels = c("1", "2", "3"),
    ),
    separation_within_production_units = factor(
      case_when(
        separation_within_production_units == 2 ~ "1",
        separation_within_production_units == 3 ~ "2",
        separation_within_production_units %in% c(1, 4) ~ "3",
        TRUE ~ NA_character_
      ),
      levels = c("1", "2", "3"),
    ),
    separation_quarantine_area = factor(
      case_when(
        separation_quarantine_area == 2 ~ "1",
        separation_quarantine_area == 3 ~ "2",
        separation_quarantine_area %in% c(1, 4) ~ "3",
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
    fattening_pigs_reduced_general_wellbeing = fattening_pigs_reduced_general_wellbeing != 0,
    fattening_pigs_discharge = fattening_pigs_discharge != 0,
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
    gestation_sows_airspace_with_other_agegroup = factor(case_when(
      is.na(gestation_sows_airspace_with_other_agegroup) ~ NA_character_,
      gestation_sows_airspace_with_other_agegroup == 2 ~ "1",
      gestation_sows_airspace_with_other_agegroup == 3 ~ "2",
      gestation_sows_airspace_with_other_agegroup == 4 ~ "3"
    ), levels = c("1", "2", "3"))
  ) %>%
  # 3. Remove columns
  dplyr::select(
    -any_of(c(
      

      #redundant or not needed
      "canton",  "production_type", "source_of_contact", "source_of_contact_grouped", "frequency_respiratory_disease_outbreaks",
      "other_animals", "quarantine_suckling_piglets", "quarantine_weaners", "quarantine_fattening_pigs", 
      "quarantine_young_sows", "quarantine_old_sows", "quarantine_boars", "visitors_in_stable","fattening_pigs_feeding_site_width_per_animal", "outbreak_since_examination_description",
      "symptom_type_swine", "symptom_type_human", "percent_killed_boars",
      "percent_diseased_suckling_piglets", "percent_diseased_weaners", "percent_diseased_fattening_pigs",
      "percent_diseased_young_sows", "percent_diseased_old_sows", "percent_diseased_boars",
      "percent_killed_suckling_piglets", "percent_killed_weaners", "percent_killed_fattening_pigs",
      "percent_killed_young_sows", "percent_killed_old_sows", "age_group_weeks", "weaners_animals_per_feeding_site",
      "number_of_origins_suckling_piglets", "number_of_origins_weaners", "number_of_origins_fattening_pigs",
      "number_of_origins_young_sows", "number_of_origins_old_sows", "number_of_origins_boars", "notes", "caretaker_type", "visitors_in_stable_recent", "start_time_current_outbreak", 
      "separation_between_production_units", "starting_point_current_disease", "separation_within_production_units", "verification_outside_area_contact_poultry", 
      "verification_outside_area_contact_wild_birds","verification_contact_poultry_stable","verification_outside_area_contact_wild_boars",
      

      #no variance or all NA
      "average_daily_weight_gain_weaners","Average_daily_weight_gain_fattening_pigs", "feed_conversion_rate_weaners", "influenza_diagnosis_human",
      "antiviral_treatment", "farrowing_sows_reduced_general_wellbeing", "farrowing_sows_sneezing", "farrowing_sows_nasal_discharge",
      "farrowing_piglets_nasal_discharge",
      "ai_sows_reduced_general_wellbeing", "ai_sows_sneezing", "ai_sows_coughing", "ai_sows_discharge",
      "gestation_sows_reduced_general_wellbeing", "gestation_sows_sneezing", "gestation_sows_coughing", "gestation_sows_discharge","sgd_qgs","average_daily_weight_gain_fattening_pigs",
      "report_killed_suckling_piglets", "farrowing_sows_coughing", "fattening_pigs_discharge", 
      
      #data integrity concerns as answers of farmers where mostly too inaccurate
      "verification_isolation_respiratory_diseased_pigs","isolation_respiratory_diseased_pigs","visitors_contact_poultry"
      
       
    ))
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
    fattening_pigs_diseased, .after = weaners_diseased
  ) %>%
  relocate(
    young_sows_diseased, .after = fattening_pigs_diseased
  ) %>%
  relocate(
    old_sows_diseased, .after =  young_sows_diseased
  ) %>%
  relocate(
    boars_diseased, .after = old_sows_diseased
  ) %>%
  relocate(
    report_killed_fattening_pigs, .after = report_killed_weaners
  ) %>%
  relocate(
    report_killed_young_sows, .after = report_killed_fattening_pigs
  ) %>%
  relocate(
    report_killed_old_sows, .after = report_killed_young_sows
  ) %>%
  relocate(
    outside_area, .before = outside_area_ai_centre
  ) %>%
  relocate(
    season_sampling, .after = date_sampling
  ) %>%
  relocate(respiratory_signs, .after = Fattening_on_farm)



# ------------------------------------------------------------------------------
# Quick overview (optional: uncomment for interactive use)
# glimpse(df3)
library(openxlsx)
write.xlsx(df3, "df3_table.xlsx")
sapply(df3, class)
# ------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Render R Markdown for descriptive statistics report.
# - The report 'eda_report2.Rmd' uses df3 from the global environment.
# - Output file is saved in the '04_output' folder.
# ------------------------------------------------------------------------------
rmarkdown::render(
"eda_report2.Rmd", 
output_file = "siv_farms_descriptive_stats.html", 
output_dir = "04_output", 
envir = globalenv()
)



# =====================================================================
# VISUAL SUMMARY PLOTS FOR KEY VARIABLES IN df3
# ---------------------------------------------------------------------
# Creates and saves (in output_folder):
#   - Pie charts for IAV_positive, respiratory_signs (TRUE vs FALSE)
#   - Bar chart for percent_positive_pigs (excluding 0)
#   - Box plots for mean_ct and number_weaners (grouped)
#   - Proportion bar plots for contact_bird_in_stable and respiratory_history_human
#   - Unified color scheme, no plot titles, SVG output for publication
# =====================================================================

library(dplyr)
library(ggplot2)
library(ggbeeswarm)
library(scales)

# ------------------ Output Folder -----------------------
output_folder <- "04_output_svg"
if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)

# ------------------ Shared Aesthetics --------------------
cb_colors <- c("TRUE" = "#4DAF4A", "FALSE" = "#E41A1C")  # colorblind-friendly
base_font <- 16

theme_pub <- theme_minimal(base_size = base_font) +
  theme(
    plot.title = element_blank(),
    legend.position = "top",
    legend.title = element_blank(),
    axis.title = element_text(size = 14),
    axis.text = element_text(size = 12)
  )

# =====================================================================
# PIE CHART FUNCTION (TRUE vs FALSE proportions)
# =====================================================================
save_pie_chart <- function(df, var, filename) {
  tab <- df %>%
    mutate(!!var := factor(.data[[var]], levels = c(TRUE, FALSE))) %>%
    group_by(.data[[var]]) %>%
    summarise(n = n()) %>%
    mutate(
      perc = n / sum(n) * 100,
      label = paste0(as.character(.data[[var]]), "\n", sprintf("%.1f%%", perc))
    )
  
  gg <- ggplot(tab, aes(x = "", y = perc, fill = .data[[var]])) +
    geom_col(width = 1) +
    coord_polar(theta = "y") +
    geom_text(aes(label = label), position = position_stack(vjust = 0.5), size = 5) +
    scale_fill_manual(values = cb_colors) +
    theme_void() +
    theme(legend.position = "none")
  
  ggsave(filename, gg, width = 6, height = 6, device = "svg")
}

# =====================================================================
# BAR CHART FUNCTION (counts for non-zero discrete variable)
# =====================================================================
save_bar_chart <- function(df, var, filename) {
  tab <- df %>%
    filter(!is.na(.data[[var]]), .data[[var]] > 0) %>%
    count(.data[[var]])
  
  gg <- ggplot(tab, aes(x = .data[[var]], y = n)) +
    geom_col(fill = "#4DAF4A", width = 0.7) +
    labs(x = var, y = "Count") +
    theme_pub
  
  ggsave(filename, gg, width = 8, height = 6, device = "svg")
}

# =====================================================================
# BOX PLOT FUNCTION (continuous by binary grouping)
# =====================================================================
save_box_plot <- function(df, value_var, group_var, filename) {
  df <- df %>%
    filter(is.finite(.data[[value_var]]), !is.na(.data[[group_var]])) %>%
    mutate(!!group_var := factor(.data[[group_var]], levels = c(TRUE, FALSE)))
  
  gg <- ggplot(df, aes(x = .data[[group_var]], y = .data[[value_var]], fill = .data[[group_var]])) +
    geom_boxplot(width = 0.6, outlier.colour = "grey70") +
    scale_fill_manual(values = cb_colors) +
    theme_pub +
    theme(legend.position = "none")
  
  ggsave(filename, gg, width = 8, height = 6, device = "svg")
}

# =====================================================================
# PROPORTION BAR PLOT FUNCTION (binary × binary)
# =====================================================================
save_prop_barplot <- function(df, var_x, var_fill, filename, x_label) {
  tab <- df %>%
    filter(!is.na(.data[[var_x]]), !is.na(.data[[var_fill]])) %>%
    group_by(.data[[var_x]], .data[[var_fill]]) %>%
    summarise(n = n(), .groups = "drop") %>%
    group_by(.data[[var_x]]) %>%
    mutate(prop = n / sum(n)) %>%
    ungroup()
  
  gg <- ggplot(tab, aes(
    x = factor(.data[[var_x]], levels = c(TRUE, FALSE)),
    y = prop,
    fill = factor(.data[[var_fill]], levels = c(TRUE, FALSE))
  )) +
    geom_col(width = 0.6, position = "fill") +
    geom_text(aes(label = percent(prop, accuracy = 1)),
              position = position_stack(vjust = 0.5),
              size = 5, color = "white") +
    scale_fill_manual(values = cb_colors, labels = c("positive", "negative")) +
    scale_x_discrete(labels = c("TRUE" = "Yes", "FALSE" = "No")) +
    scale_y_continuous(labels = percent_format(accuracy = 1)) +
    labs(x = x_label, y = "Proportion") +
    theme_pub
  
  ggsave(filename, gg, width = 8, height = 6, device = "svg")
}


# =====================================================================
# ======================= GENERATE ALL PLOTS ==========================
# =====================================================================

# PIE CHARTS
save_pie_chart(df3, "IAV_positive", file.path(output_folder, "pie_IAV_positive.svg"))
save_pie_chart(df3, "respiratory_signs", file.path(output_folder, "pie_respiratory_signs.svg"))

# BAR CHART: percentage of positive pigs
save_bar_chart(df3, "percent_positive_pigs", file.path(output_folder, "bar_percent_positive_pigs.svg"))


# BOX PLOT: number of weaners by SIV status
save_box_plot(df3, "number_weaners", "IAV_positive", file.path(output_folder, "box_number_weaners_by_SIV_status.svg"))

# PROPORTION BAR PLOT: SIV status by bird contact
save_prop_barplot(df3, "contact_bird_in_stable", "IAV_positive",
                  file.path(output_folder, "prop_SIV_by_contact_bird_in_stable.svg"),
                  x_label = "Contact with birds in stable")

# PROPORTION BAR PLOT: SIV status by human respiratory history
save_prop_barplot(df2, "respiratory_history_human", "IAV_positive",
                  file.path(output_folder, "prop_SIV_by_respiratory_history_human.svg"),
                  x_label = "Respiratory history (human)")


cat("All visual summary plots successfully exported as SVG to", output_folder, "\n")
