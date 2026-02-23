############################################################
# Swiss Pig SIV Risk Factors: Data Preparation and PLS-DA Analysis
# Subset Analysis Extension
# Author: jonasalexandersteiner
#
# This script performs:
# 1. Data preparation for PLS-DA, including:
#    1. Restrict input to subset-defined variables (only Husbandry, Animals, Environment, Human/Contact)
#    2. Custom binning and factor conversions (with explicit NA handling only when NA present)
#    3. Conversion of sneezing/coughing variables to logicals (with NA preserved and recoded)
#    4. Handling of missing values in numeric columns (imputation or removal)
#    5. Conversion of ALL logical and factor variables to factors with levels TRUE, FALSE, NA (NA only if present)
#    6. Dummy coding of multi-level factors (proper reference columns dropped, robust NA and FALSE handling)
#    7. Construction of the final numeric predictor matrix X (all predictors numeric and ready for modeling)
# 2. Systematic predictor filtering for modeling
#    1. Low-variance and constant columns (nearZeroVar from caret)
#    2. Columns with all NA values
#    3. Columns with only one unique value (after NA removal)
#    4. Impute selected NAs with 0
#    5. Exact duplicate columns
#    6. Highly correlated columns (>0.95, standard)
#    7. Save predictor removal log
# 3. Preparation of response variable for PLS-DA
# 4. Standardization of predictors and export
#    1. For each subset (Husbandry, Animals, Environment, Human/Contact), selects relevant predictors, standardizes them, and exports the matrix and response for reproducibility
# 5. PLS-DA model fitting and variable importance extraction
#    1. For each subset, fits the PLS-DA model, performs cross-validation for component selection, extracts variable importance (VIP), scores, loadings, explained variance, and stores results
# 6. Saving of analysis results and rendering of visualization report
#    1. Saves all subset model results and renders a single R Markdown visualization report for all subsets
############################################################

# --- VERSION AND REPRODUCIBILITY INFO ---
script_version <- "v1.3.2"
analysis_git_hash <- tryCatch(system("git rev-parse HEAD", intern=TRUE), error = function(e) NA)
set.seed(123)  # global seed for reproducibility across the whole script
cat("Script version:", script_version, "\n")
cat("Git commit hash:", analysis_git_hash, "\n")
cat("Random seed for modeling and cross-validation: 123\n\n")

# ---- 0. Load Required Libraries ----
library(tidyverse)  # Data manipulation
library(caret)      # Dummy encoding, filtering, modeling utilities
library(mixOmics)   # PLS-DA modeling
library(readr)      # UTF-8-safe CSV writing
library(stringr)    # String helpers (robust matching)

# Ensure UTF-8 output (helps keep en-dash "–" intact in logs)
try(suppressWarnings(Sys.setlocale("LC_CTYPE", "en_US.UTF-8")), silent = TRUE)

# ---- 0a. Helper Functions (only those not defined upstream) ----
# NOTE: make_exclude_vars(), get_husbandry_vars(), get_animal_vars(),
#       get_environment_vars(), get_human_vars()
# are defined in the preceding inferential script and reused here.

cut_with_na <- function(x, breaks, labels, right=FALSE) {
  y <- cut(x, breaks=breaks, labels=labels, right=right, include.lowest=TRUE)
  addNA(y, ifany=TRUE)
}

# Best-practice: automatically build dummy_factors when not provided
build_dummy_factors <- function(df, extra_exclude = character()) {
  # candidate categorical predictors: factor, logical, character
  cand <- names(df)[sapply(df, function(x) is.factor(x) || is.logical(x) || is.character(x))]
  # default exclusions: outcome and obvious IDs if present
  default_exclude <- c("IAV_positive", "outcome", "target", "id", "ID", "sample_id", "farm_id")
  setdiff(cand, unique(c(default_exclude, extra_exclude)))
}

# Robust dummy reference dropping helpers
normalize_variants <- function(lbl) {
  base <- c(lbl, gsub("–","-", lbl), gsub("-", "–", lbl))
  mn <- unique(make.names(base))
  xpref <- unique(paste0("X", mn))
  unique(c(base, mn, xpref))
}
find_dummy_col <- function(dummy_df, df_data, var, level_label) {
  cands <- grep(paste0("^", var, "_"), names(dummy_df), value = TRUE)
  if (!length(cands)) return(NA_character_)
  rem <- sub(paste0("^", var, "_"), "", cands)
  
  variants <- normalize_variants(level_label)
  idx <- which(rem %in% variants)
  if (length(idx)) return(cands[idx[1]])
  
  collapse <- function(s) tolower(gsub("[^[:alnum:]]", "", s))
  idx <- which(collapse(rem) == collapse(level_label))
  if (length(idx)) return(cands[idx[1]])
  
  if (!is.null(df_data) && var %in% names(df_data)) {
    x <- df_data[[var]]
    if (is.logical(x)) {
      x <- ifelse(is.na(x), "NA", as.character(x))
    } else if (is.factor(x)) {
      x <- as.character(x); x[is.na(x)] <- "NA"
    } else if (is.character(x)) {
      x[is.na(x)] <- "NA"
    } else {
      return(NA_character_)
    }
    for (j in seq_along(cands)) {
      col <- cands[j]
      idx1 <- which(dummy_df[[col]] == 1)
      if (!length(idx1)) next
      dom <- names(sort(table(x[idx1]), decreasing = TRUE))[1]
      if (!is.na(dom) && dom == level_label) return(col)
      if (collapse(dom) == collapse(level_label)) return(col)
      dom_variants <- normalize_variants(dom)
      if (length(intersect(dom_variants, variants)) > 0) return(col)
    }
  }
  NA_character_
}
drop_reference_level <- function(dummy_df, df_data, var, level_label,
                                 removed_custom, removed_custom_reason, removed_custom_var, reason_text) {
  col_name <- find_dummy_col(dummy_df, df_data, var, level_label)
  if (!is.na(col_name) && col_name %in% names(dummy_df)) {
    dummy_df[[col_name]] <- NULL
    removed_custom <- c(removed_custom, col_name)
    removed_custom_reason <- c(removed_custom_reason, reason_text)
    removed_custom_var <- c(removed_custom_var, var)
  } else {
    message(sprintf("Reference not found for %s level '%s' (name/content match failed)", var, level_label))
  }
  list(dummy_df = dummy_df,
       removed_custom = removed_custom,
       removed_custom_reason = removed_custom_reason,
       removed_custom_var = removed_custom_var)
}

# Regex-escape for exact prefix matching with underscores
fixed_rx <- function(x) stringr::str_replace_all(x, "([\\^$.*+?()\\[\\]{}|\\\\])", "\\\\\\1")

# ===============================
# 1. DATA PREPARATION STAGE
# ===============================

# 1.1. Make Working Copy of Input Data
df3_pls <- df3

# 1.1.a Keep only subset-defined variables (plus outcome and farm_id)
# Build subset roots using upstream helpers (exclude meta + overview-dependent + outcome)
exclude_vars <- make_exclude_vars("IAV_positive")
vars_husbandry   <- get_husbandry_vars(exclude_vars)
vars_animals     <- get_animal_vars(exclude_vars)
vars_environment <- get_environment_vars(exclude_vars)
vars_human       <- get_human_vars(exclude_vars)

# Add explicit derived predictors (TRUE dummies to keep) to Animals roots NOW
extra_predictors <- c(
  "farrowing_piglet_litters_sneezing_TRUE",
  "farrowing_piglet_litters_coughing_TRUE"
)
vars_animals <- unique(c(vars_animals, extra_predictors))

# Union of allowed variable roots (subset families)
allowed_roots <- unique(c(vars_husbandry, vars_animals, vars_environment, vars_human))

# Explicitly banned overview-dependent variables (must NOT enter df3_pls)
banned_roots <- c(
  "quarantine_time",
  "outside_area_ai_centre",
  "outside_area_gestation_stable",
  "outside_area_farrowing_stable",
  "outside_area_weaner_stable",
  "outside_area_fattening_stable"
)

# Resolve to concrete column names via exact "root or root_" prefix matching
grep_prefix <- function(vars, cols) {
  if (length(vars) == 0) return(character())
  unique(unlist(lapply(vars, function(v) {
    grep(paste0("^", fixed_rx(v), "($|_)"), cols, value = TRUE)
  })))
}
allowed_cols <- grep_prefix(allowed_roots, names(df3))
banned_cols  <- grep_prefix(banned_roots,  names(df3))

# Drop banned columns from the allowlist (prevents prefix bleed like "outside_area_*")
allowed_cols <- setdiff(allowed_cols, banned_cols)

# Keep outcome and farm_id for transformations and modeling
must_keep <- intersect(c("IAV_positive", "farm_id"), names(df3))

# Rebuild df3_pls to only these columns
kept_cols <- unique(c(allowed_cols, must_keep))
dropped_cols <- setdiff(names(df3_pls), kept_cols)
df3_pls <- df3_pls[, kept_cols, drop = FALSE]

message("Subset filter applied at start:")
message(" - Kept columns: ", paste(kept_cols, collapse = ", "))
if (length(dropped_cols)) {
  message(" - Dropped columns: ", paste(dropped_cols, collapse = ", "))
} else {
  message(" - No columns were dropped.")
}

# 1.2. Apply Custom Binning and Factor Conversion
df3_pls <- df3_pls %>%
  mutate(
    across(matches("_air_quality$"), as.factor),
    
    # Create logicals for piglet litter symptoms; preserve NA
    farrowing_piglet_litters_sneezing = ifelse(
      is.na(farrowing_piglets_sneezing), NA, farrowing_piglets_sneezing > 0
    ),
    farrowing_piglet_litters_coughing = ifelse(
      is.na(farrowing_piglets_coughing), NA, farrowing_piglets_coughing > 0
    )
  ) %>%
  mutate(
    farrowing_room_temperature = cut_with_na(
      farrowing_room_temperature, breaks = c(-Inf, 17, 20, 23, 26, Inf),
      labels = c("<17", "17–20", "20–23", "23–26", ">26")
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
    gestation_sows_qm_per_animal = cut_with_na(
      gestation_sows_qm_per_animal, breaks = c(-Inf, 2.5, 3.5, 4.5, 6.0, Inf),
      labels = c("<2.5", "2.5–<3.5", "3.5–<4.5", "4.5–<6.0", ">=6.0")
    ),
    gestation_sows_animals_per_water_source = cut_with_na(
      gestation_sows_animals_per_water_source, breaks = c(-Inf, 4, 8, 12, 16, Inf),
      labels = c("<4", "4–<8", "8–<12", "12–<16", ">=16")
    ),
    gestation_sows_room_temperature = cut_with_na(
      gestation_sows_room_temperature, breaks = c(-Inf, 13, 17, 21, 25, Inf),
      labels = c("<13", "13–<17", "17–<21", "21–<25", ">=25")
    ),
    gestation_sows_airflow = cut_with_na(
      gestation_sows_airflow, breaks = c(-Inf, 0.1, 0.2, 0.4, Inf),
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
    rectal_temperature_max = cut_with_na(
      rectal_temperature_max, breaks = c(-Inf, 40.0, 40.5, 41.0, Inf),
      labels = c("<40.0", "40.0–<40.5", "40.5–<41.0", ">=41.0")
    ),
    rectal_temperature_avg = cut_with_na(
      rectal_temperature_avg, breaks = c(-Inf, 40.0, 40.5, 41.0, Inf),
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
    fattening_pigs_sneezing = if_else(farm_id %in% 1:3, NA_real_, fattening_pigs_sneezing),
    fattening_pigs_sneezing = cut_with_na(
      fattening_pigs_sneezing, breaks = c(-Inf, 0, 2, 5, Inf),
      labels = c("0", "0–<2", "2–<5", "5+")
    ),
    fattening_pigs_coughing = if_else(farm_id %in% 1:3, NA_real_, fattening_pigs_coughing),
    fattening_pigs_coughing = cut_with_na(
      fattening_pigs_coughing, breaks = c(-Inf, 0, 2, 5, Inf),
      labels = c("0", "0–<2", "2–<5", "5+")
    ),
    fattening_pigs_qm_per_animal = if_else(farm_id %in% 1:6, NA_real_, fattening_pigs_qm_per_animal),
    fattening_pigs_qm_per_animal = cut_with_na(
      fattening_pigs_qm_per_animal, breaks = c(-Inf, 0.5, 1.0, 1.5, Inf),
      labels = c("<0.5", "0.5–<1.0", "1.0–<1.5", ">=1.5")
    ),
    fattening_pigs_animals_per_water_source = if_else(farm_id %in% 1:6, NA_real_, fattening_pigs_animals_per_water_source),
    fattening_pigs_animals_per_water_source = cut_with_na(
      fattening_pigs_animals_per_water_source, breaks = c(-Inf, 10, 15, Inf),
      labels = c("<10", "10–<15", ">=15")
    ),
    fattening_pigs_room_temperature = if_else(farm_id %in% 1:3, NA_real_, fattening_pigs_room_temperature),
    fattening_pigs_room_temperature = cut_with_na(
      fattening_pigs_room_temperature, breaks = c(-Inf, 10, 15, 20, 25, Inf),
      labels = c("<10", "10–<15", "15–<20", "20–<25", ">=25")
    ),
    fattening_pigs_airflow = if_else(farm_id %in% c(1:3,7), NA_real_, fattening_pigs_airflow),
    fattening_pigs_airflow = cut_with_na(
      fattening_pigs_airflow, breaks = c(-Inf, 0.1, 0.2, 0.4, Inf),
      labels = c("<0.1", "0.1–<0.2", "0.2–<0.4", ">=0.4")
    )
  ) %>%
  # Ensure outside_area* are logical (so we get "..._TRUE" dummies for allowed ones)
  mutate(
    across(starts_with("outside_area"), ~ ifelse(is.na(.), NA, . > 0))
  )

# 1.3. Handle Missing Values in specified Numeric Columns
numeric_na_to_zero <- c(
  "number_suckling_piglets", "number_weaners", "number_fattening_pigs",
  "number_young_sows", "number_old_sows", "number_boars"
)
for (col in numeric_na_to_zero) {
  if (col %in% names(df3_pls)) {
    df3_pls[[col]][is.na(df3_pls[[col]])] <- 0
  }
}

# 1.4. Identify Factors for Dummy Coding (excluding meta variables)
meta_vars <- c("farm_id", "total_samples", "positive_pigs",
               "percent_positive_pigs", "date_sampling", "IAV_positive",
               "min_ct", "max_ct", "mean_ct", "sd_ct", "source_of_contact_grouped")
is_dummy_factor <- sapply(df3_pls, function(x) {
  (is.factor(x) && !all(is.na(x)) && nlevels(x) > 1) || is.logical(x)
})
dummy_factors <- setdiff(names(df3_pls)[is_dummy_factor], meta_vars)

# 1.5. Convert logicals and factors in dummy_factors to factors with levels TRUE, FALSE, NA (NA only if present)
df3_pls[dummy_factors] <- lapply(df3_pls[dummy_factors], function(x) {
  if (is.logical(x)) {
    vals <- ifelse(is.na(x), "NA", as.character(x))
    levels_needed <- unique(vals)
    factor(vals, levels = levels_needed)
  } else if (is.factor(x)) {
    x_char <- as.character(x)
    x_char[is.na(x_char)] <- "NA"
    levels_needed <- unique(x_char)
    factor(x_char, levels = levels_needed)
  } else {
    x
  }
})

# 1.6. Dummy Encode Multi-level Factors and remove baseline dummy per variable (with logging)
if (!exists("dummy_factors") || length(dummy_factors) == 0) {
  dummy_factors <- names(df3_pls)[sapply(df3_pls, function(x) is.factor(x) || is.logical(x) || is.character(x))]
  dummy_factors <- setdiff(dummy_factors, c("IAV_positive"))
}

if (length(dummy_factors) > 0) {
  dv <- caret::dummyVars(
    formula = as.formula(paste("~", paste(dummy_factors, collapse = " + "))),
    data = df3_pls,
    sep = "_",
    fullRank = FALSE
  )
  dummy_df <- as.data.frame(predict(dv, newdata = df3_pls))
} else {
  dummy_df <- NULL
}

removed_custom <- character()
removed_custom_reason <- character()
removed_custom_var <- character()
removed_log_rows <- list()

if (!is.null(dummy_df) && ncol(dummy_df) > 0) {
  # Robust outcome coding to 0/1
  to_y_binary <- function(y) {
    yy <- y
    if (!is.factor(yy)) yy <- factor(yy)
    as.numeric(ifelse(yy %in% c(1, "1", TRUE, "TRUE", "positive"), 1,
                      ifelse(yy %in% c(0, "0", FALSE, "FALSE", "negative"), 0, NA)))
  }
  # Clopper–Pearson exact CI
  cp_ci <- function(k, n, conf = 0.95) {
    if (n <= 0) return(c(NA_real_, NA_real_))
    bt <- tryCatch(binom.test(k, n, conf.level = conf), error = function(e) NULL)
    if (is.null(bt)) return(c(NA_real_, NA_real_))
    bt$conf.int
  }
  # Per-level rates
  compute_level_rates <- function(df, var, ybin) {
    if (!(var %in% names(df))) return(NULL)
    x <- df[[var]]
    if (is.logical(x)) {
      vals <- ifelse(is.na(x), "NA", as.character(x))
      x <- factor(vals, levels = unique(vals))
    } else if (!is.factor(x)) {
      if (is.character(x)) x <- factor(ifelse(is.na(x), "NA", x))
      else return(NULL)
    }
    dplyr::tibble(level = x, y = ybin) %>%
      dplyr::filter(!is.na(y), !is.na(level), level != "NA") %>%
      dplyr::group_by(level) %>%
      dplyr::summarise(
        n = dplyr::n(),
        pos = sum(y == 1),
        neg = sum(y == 0),
        rate = pos / n,
        ci_low = cp_ci(pos, n)[1],
        ci_high = cp_ci(pos, n)[2],
        .groups = "drop"
      )
  }
  
  # Configuration
  min_n_level    <- 10
  min_pos_level  <- 3
  min_neg_level  <- 3
  
  # Force baseline to FALSE so TRUE dummies are retained for these derived variables
  override_map <- c(
    "farrowing_piglet_litters_sneezing" = "FALSE",
    "farrowing_piglet_litters_coughing" = "FALSE"
  )
  
  y_bin <- to_y_binary(df3_pls$IAV_positive)
  
  # Variables that produced at least one dummy
  cat_vars <- names(df3_pls)[sapply(df3_pls, function(x) is.factor(x) || is.logical(x) || is.character(x))]
  produced_vars <- Filter(function(v) any(grepl(paste0("^", fixed_rx(v), "_"), names(dummy_df))), cat_vars)
  
  cat(sprintf("Vars with produced dummies: %d\n", length(produced_vars)))
  
  for (var in produced_vars) {
    override_level <- NULL
    if (var %in% names(override_map)) {
      var_levels <- unique(as.character(df3_pls[[var]]))
      if (override_map[[var]] %in% var_levels) override_level <- override_map[[var]]
    }
    
    tab <- compute_level_rates(df3_pls, var, y_bin)
    if (is.null(tab) || nrow(tab) == 0) {
      message(sprintf("1.6: Auto-select skipped for %s (no levels).", var)); next
    }
    
    eligible <- tab %>% dplyr::filter(n >= min_n_level, pos >= min_pos_level, neg >= min_neg_level)
    method <- "upper_ci"
    full_pick <- if (nrow(eligible) > 0) {
      eligible %>% dplyr::arrange(ci_high, rate, dplyr::desc(n)) %>% dplyr::slice(1)
    } else {
      method <- "rate_fallback"
      tab %>% dplyr::arrange(rate, dplyr::desc(n)) %>% dplyr::slice(1)
    }
    
    chosen_level_final <- if (!is.null(override_level)) { method <- "override"; override_level } else { as.character(full_pick$level) }
    
    final_row <- tab %>% dplyr::filter(level == chosen_level_final)
    final_rate <- if (nrow(final_row)) as.numeric(final_row$rate) else NA_real_
    final_n    <- if (nrow(final_row)) as.integer(final_row$n) else NA_integer_
    final_pos  <- if (nrow(final_row)) as.integer(final_row$pos) else NA_integer_
    final_neg  <- if (nrow(final_row)) as.integer(final_row$neg) else NA_integer_
    final_lci  <- if (nrow(final_row)) as.numeric(final_row$ci_low) else NA_real_
    final_uci  <- if (nrow(final_row)) as.numeric(final_row$ci_high) else NA_real_
    
    pre_count <- length(grep(paste0("^", fixed_rx(var), "_"), names(dummy_df)))
    res <- drop_reference_level(
      dummy_df, df3_pls, var, chosen_level_final,
      removed_custom, removed_custom_reason, removed_custom_var,
      reason_text = sprintf("Auto-selected baseline '%s' by %s (rate=%.3f, UCI=%.3f, n=%d)",
                            chosen_level_final, method, final_rate, final_uci, final_n)
    )
    dummy_df <- res$dummy_df
    removed_custom <- res$removed_custom
    removed_custom_reason <- res$removed_custom_reason
    removed_custom_var <- res$removed_custom_var
    post_count <- length(grep(paste0("^", fixed_rx(var), "_"), names(dummy_df)))
    
    removed_dummy_name <- if (length(removed_custom)) utils::tail(removed_custom, 1) else NA_character_
    expected_dummy_name <- paste0(var, "_", make.names(chosen_level_final))
    removal_status <- if (isTRUE(pre_count - post_count == 1)) "removed" else "failed"
    if (!isTRUE(pre_count - post_count == 1) && is.na(removed_dummy_name)) removed_dummy_name <- expected_dummy_name
    
    removed_log_rows[[length(removed_log_rows) + 1]] <- data.frame(
      SourceVariable = var,
      BaselineLevel = chosen_level_final,
      DummyColumn = removed_dummy_name,
      n = final_n,
      pos = final_pos,
      neg = final_neg,
      rate = final_rate,
      ci_low = final_lci,
      ci_high = final_uci,
      SelectionMethod = method,
      OverrideApplied = identical(method, "override"),
      RemovalStatus = removal_status,
      Reason = sprintf("Auto-selected baseline '%s' by %s", chosen_level_final, method),
      stringsAsFactors = FALSE
    )
  }
}

removed_log <- if (length(removed_log_rows)) do.call(rbind, removed_log_rows) else {
  data.frame(
    SourceVariable = character(),
    BaselineLevel = character(),
    DummyColumn = character(),
    n = integer(),
    pos = integer(),
    neg = integer(),
    rate = numeric(),
    ci_low = numeric(),
    ci_high = numeric(),
    SelectionMethod = character(),
    OverrideApplied = logical(),
    RemovalStatus = character(),
    Reason = character(),
    stringsAsFactors = FALSE
  )
}
readr::write_csv(removed_log, "removed_reference_dummies_log.csv")

cat("\n1.6 summary: removed reference dummies\n")
if (nrow(removed_log) > 0) {
  apply(removed_log, 1, function(r) {
    cat(sprintf(" - %s: %s | baseline='%s' | rate=%.3f UCI=%.3f n=%s | status=%s | %s\n",
                r[["SourceVariable"]], r[["DummyColumn"]], r[["BaselineLevel"]],
                as.numeric(r[["rate"]]), as.numeric(r[["ci_high"]]), r[["n"]],
                r[["RemovalStatus"]], r[["Reason"]]))
  })
} else {
  cat(" - none removed in this run\n")
}

# Bind dummies to df3_pls
if (!is.null(dummy_df) && ncol(dummy_df) > 0) {
  df3_pls <- df3_pls %>%
    dplyr::select(-all_of(dummy_factors)) %>%
    dplyr::bind_cols(dummy_df)
}

# 1.7. Build Final Numeric Predictor Matrix X
# Use any_of to avoid errors if some meta vars were never present after early filtering
# Also explicitly drop banned overview-dependent roots at this stage (belt-and-suspenders)
X <- df3_pls %>%
  dplyr::select(
    -tidyselect::any_of(meta_vars),
    -tidyselect::any_of(banned_roots),
    -dplyr::starts_with("verification")
  ) %>%
  dplyr::mutate(across(where(is.logical), as.numeric)) %>%
  dplyr::mutate(across(where(is.factor), ~ as.numeric(as.character(.)))) %>%
  dplyr::mutate(across(where(is.character), ~ suppressWarnings(as.numeric(.)))) %>%
  dplyr::mutate(across(where(is.integer), as.numeric)) %>%
  as.data.frame()

# ===============================
# 2. PREPARE RESPONSE VARIABLE
# ===============================
Y_raw <- df3_pls$IAV_positive
if (!is.factor(Y_raw)) Y_raw <- as.factor(Y_raw)
Y <- factor(
  ifelse(
    Y_raw %in% c(1, "1", TRUE, "TRUE", "positive"), TRUE,
    ifelse(Y_raw %in% c(0, "0", FALSE, "FALSE", "negative"), FALSE, NA)
  ),
  levels = c(FALSE, TRUE)
)
if (!is.factor(Y)) stop("IAV_positive could not be coerced to a factor for PLS-DA.")
if (length(levels(Y)) != 2) stop("IAV_positive must have exactly two levels (TRUE/FALSE) for binary PLS-DA.")
if (any(is.na(Y))) {
  valid_idx <- !is.na(Y)
  X <- X[valid_idx, , drop = FALSE]
  Y <- Y[valid_idx]
}

# ===============================
# 3. SYSTEMATIC PREDICTOR FILTERING
# ===============================
removed_predictors_log <- data.frame(
  Predictor = character(),
  Reason = character(),
  DuplicateOf = character(),
  AliasedWith = character(),
  stringsAsFactors = FALSE
)

# 3.1. NZV / constant
nzv_metrics <- caret::nearZeroVar(X, saveMetrics = TRUE)
problem_idx <- which(nzv_metrics$zeroVar | nzv_metrics$nzv)
cols_to_remove <- rownames(nzv_metrics)[problem_idx]
if(length(cols_to_remove) > 0) {
  removed_predictors_log <- rbind(
    removed_predictors_log,
    data.frame(Predictor = cols_to_remove, Reason = "Near-zero or zero variance",
               DuplicateOf = NA_character_, AliasedWith = NA_character_, stringsAsFactors = FALSE)
  )
  X <- X[, !colnames(X) %in% cols_to_remove, drop = FALSE]
}

# 3.2. All NA columns
all_na_cols <- colnames(X)[colSums(!is.na(X)) == 0]
if(length(all_na_cols) > 0) {
  removed_predictors_log <- rbind(
    removed_predictors_log,
    data.frame(Predictor = all_na_cols, Reason = "All NA values",
               DuplicateOf = NA_character_, AliasedWith = NA_character_, stringsAsFactors = FALSE)
  )
  X <- X[, colSums(!is.na(X)) > 0, drop = FALSE]
}

# 3.3. Constant after NA removal
constant_cols <- colnames(X)[sapply(X, function(x) length(unique(x[!is.na(x)])) <= 1)]
if(length(constant_cols) > 0) {
  removed_predictors_log <- rbind(
    removed_predictors_log,
    data.frame(Predictor = constant_cols, Reason = "Only one unique value",
               DuplicateOf = NA_character_, AliasedWith = NA_character_, stringsAsFactors = FALSE)
  )
  X <- X[, sapply(X, function(x) length(unique(x[!is.na(x)])) > 1), drop = FALSE]
}

# 3.4. Impute remaining NAs with 0 (for modeling)
if (any(is.na(X))) X[is.na(X)] <- 0

# 3.5. Exact duplicate columns
dup_cols_idx <- which(duplicated(t(X)))
if(length(dup_cols_idx) > 0) {
  dup_cols <- colnames(X)[dup_cols_idx]
  for (i in seq_along(dup_cols_idx)) {
    idx <- dup_cols_idx[i]
    dup_name <- colnames(X)[idx]
    orig_idx <- which(apply(t(X)[1:(idx-1),,drop=FALSE], 1, function(row) all(row == X[,idx])))
    orig_name <- if (length(orig_idx) > 0) colnames(X)[orig_idx[1]] else NA_character_
    removed_predictors_log <- rbind(
      removed_predictors_log,
      data.frame(Predictor = dup_name, Reason = "Duplicate predictor",
                 DuplicateOf = orig_name, AliasedWith = NA_character_, stringsAsFactors = FALSE)
    )
  }
  X <- X[, !duplicated(t(X)), drop=FALSE]
}

# 3.6. Highly correlated (>0.95)
cor_matrix <- suppressWarnings(cor(X))
if (any(is.na(cor_matrix))) {
  badcols <- unique(c(which(rowSums(is.na(cor_matrix)) > 0), which(colSums(is.na(cor_matrix)) > 0)))
  if (length(badcols) > 0) {
    cols_to_remove <- colnames(X)[badcols]
    removed_predictors_log <- rbind(
      removed_predictors_log,
      data.frame(Predictor = cols_to_remove, Reason = "NA in correlation matrix (likely zero variance)",
                 DuplicateOf = NA_character_, AliasedWith = NA_character_, stringsAsFactors = FALSE)
    )
    X <- X[, -badcols, drop = FALSE]
    cor_matrix <- suppressWarnings(cor(X))
  }
}
highly_correlated <- if (ncol(X) > 1) findCorrelation(cor_matrix, cutoff = 0.95) else integer()
if (length(highly_correlated) > 0) {
  cols_to_remove <- colnames(X)[highly_correlated]
  removed_predictors_log <- rbind(
    removed_predictors_log,
    data.frame(Predictor = cols_to_remove, Reason = "High correlation (>0.95)",
               DuplicateOf = NA_character_, AliasedWith = NA_character_, stringsAsFactors = FALSE)
  )
  X <- X[, !colnames(X) %in% cols_to_remove, drop = FALSE]
}

# 3.7. Save predictor removal log
readr::write_csv(removed_predictors_log, "removed_predictors_log.csv")

# ===============================
# 4. STANDARDIZE PREDICTORS AND EXPORT SUBSET MATRICES
# ===============================

# 4.1. Define subset variable lists (reusing helpers from the upstream script)
# extra_predictors already added to vars_animals above

# Use escaped matching for subsets
find_subset_cols <- function(vars, all_cols) {
  unique(unlist(lapply(vars, function(v)
    grep(paste0("^", fixed_rx(v), "($|_)"), all_cols, value = TRUE))))
}

subset_list <- list(
  Husbandry   = find_subset_cols(vars_husbandry,   colnames(X)),
  Animals     = find_subset_cols(vars_animals,     colnames(X)),
  Environment = find_subset_cols(vars_environment, colnames(X)),
  Human       = find_subset_cols(vars_human,       colnames(X))
)

output_dir <- "04_output/SIV_PLS_Analysis_Results"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

# Optional: export a subset-to-columns map for transparency
subset_map <- lapply(names(subset_list), function(sn) {
  data.frame(
    subset = sn,
    column = subset_list[[sn]],
    stringsAsFactors = FALSE
  )
})
subset_map <- do.call(rbind, subset_map)
readr::write_csv(subset_map, file.path(output_dir, "subset_column_map.csv"))

# 4.2. Standardize and export each subset predictor matrix and response
for (subset_name in names(subset_list)) {
  subset_cols <- subset_list[[subset_name]]
  X_subset <- X[, subset_cols, drop = FALSE]
  X_subset_std <- as.data.frame(scale(X_subset))
  readr::write_csv(X_subset_std, file.path(output_dir, paste0("model_X_", subset_name, "_used.csv")))
  readr::write_csv(data.frame(IAV_positive = Y), file.path(output_dir, paste0("model_Y_", subset_name, "_used.csv")))
}

# ===============================
# 5. SUBSET-SPECIFIC PLS-DA MODEL FITTING AND RESULT EXTRACTION
# ===============================

subset_results <- list()
for (subset_name in names(subset_list)) {
  subset_cols <- subset_list[[subset_name]]
  X_subset <- X[, subset_cols, drop = FALSE]
  valid_idx <- complete.cases(X_subset) & !is.na(Y)
  X_subset_valid <- X_subset[valid_idx, , drop = FALSE]
  Y_subset <- Y[valid_idx]
  if (is.list(Y_subset)) Y_subset <- unlist(Y_subset)
  if (is.matrix(Y_subset)) Y_subset <- as.vector(Y_subset)
  Y_subset <- as.factor(Y_subset)
  X_subset_std <- as.data.frame(scale(X_subset_valid))
  if (ncol(X_subset_std) > 1 && length(unique(Y_subset)) == 2) {
    max_comp <- min(3, ncol(X_subset_std), floor(nrow(X_subset_std)/4))
    set.seed(123)
    plsda_model <- mixOmics::plsda(X_subset_std, Y_subset, ncomp = max_comp)
    ncomp_optimal <- tryCatch({
      cv <- mixOmics::perf(plsda_model, validation = "Mfold", folds = 5, progressBar = FALSE)
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
    if ("mixo_plsda" %in% class(plsda_model)) {
      vip_scores <- mixOmics::vip(plsda_model)
      coef_df <- data.frame(
        Predictor = rownames(vip_scores),
        VIP = vip_scores[, ncomp_optimal]
      ) %>%
        dplyr::group_by(Predictor) %>%
        dplyr::slice_max(VIP, n = 1, with_ties = FALSE) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(dplyr::desc(VIP))
    } else {
      stop("The model type is incompatible with VIP score extraction using mixOmics::vip().")
    }
    loadings_df <- as.data.frame(plsda_model$loadings$X[, 1:ncomp_model, drop = FALSE])
    scores_df <- as.data.frame(plsda_model$variates$X[, 1:ncomp_model, drop = FALSE])
    colnames(scores_df) <- paste0("Comp ", 1:ncol(scores_df))
    explained_var <- plsda_model$prop_expl_var$X
    names(explained_var) <- paste0("Comp ", seq_along(explained_var))
    subset_results[[subset_name]] <- list(
      plsda_model = plsda_model,
      coef_df = coef_df,
      loadings_df = loadings_df,
      scores_df = scores_df,
      explained_var = explained_var,
      ncomp_optimal = ncomp_optimal,
      sample_size = nrow(X_subset_std),
      predictor_count = ncol(X_subset_std),
      original_predictor_count = length(subset_cols),
      date_time = Sys.time(),
      author = "jonasalexandersteiner",
      X = X_subset_std,
      IAV_positive = Y_subset
    )
  }
}

# ===============================
# 6. SAVE SUBSET ANALYSIS RESULTS AND RENDER REPORT
# ===============================

saveRDS(subset_results, file.path(output_dir, "plsda_analysis_results_subsets.rds"))

rmd_file <- file.path(output_dir, "SIV_PLS_visualization.Rmd")
html_file <- file.path(output_dir, "Supplementary_File_S5.html")
if (!file.exists(rmd_file)) stop(paste("R Markdown file does not exist:", rmd_file))
rmarkdown::render(
  input = rmd_file,
  output_file = html_file,
  output_dir = output_dir,
  envir = new.env(parent = globalenv())
)

############################################################
# REPRODUCIBILITY
############################################################

# Write session info
sink(file.path(output_dir, "R_sessionInfo.txt"))
print(sessionInfo())
sink()

cat("\n############## Recommendations for Publication ##############\n")
cat("1. Script version, git commit, and random seed are logged at the top for full reproducibility.\n")
cat("2. Subset variable definitions are harmonized with the univariate/correlation analysis and clearly documented in the upstream script.\n")
cat("3. All filtering steps are logged to 'removed_predictors_log.csv' and reference baseline removals to 'removed_reference_dummies_log.csv'.\n")
cat("4. Standardized model input matrices (X) and the binary response variable (Y) are exported per subset.\n")
cat("5. Session information is written to 'R_sessionInfo.txt' for full environment documentation.\n")
cat("6. Subset PLS-DA model results are stored in a single RDS file for downstream use.\n")
cat("7. R Markdown report renders all results with full provenance and reproducibility info.\n")
cat("\n############################################################\n")
cat("If submitting this work, include all output files, this script, and the data version/commit referenced.\n")
cat("All variable selection, dummy coding, and modeling steps are traceable in the logs and output.\n")
cat("For questions on variable selection, dummy coding, or model decisions, see the detailed logs and comments.\n")
############################################################
# End of SIV Binary PLS-DA Subset Analysis Script
############################################################