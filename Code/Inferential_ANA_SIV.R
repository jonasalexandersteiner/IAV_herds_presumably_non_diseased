# =====================================================================
# INFERENTIAL ANALYSIS: IAV_positive Only (Association + Correlation)
# ---------------------------------------------------------------------
# For IAV_positive outcome:
#   - For each subset (Husbandry, Animals, Environment, Human)
#     - Univariate association (FDR correction, interactive tables, highlight significant)
#     - Custom statistical test assignment per variable
# Output: association_IAV_positive_by_subset.html
#
# For all variables (not just significant), show correlation with IAV_positive:
#     - Spearman (numeric/binary pairs)
#     - Cramér's V (categorical pairs with >2 levels)
#     - Show: p-value, FDR-p, correlation coefficient, N
# Output: correlation_IAV_positive_by_subset.html
# =====================================================================

library(dplyr)
library(purrr)
library(DT)
library(htmltools)
library(openxlsx)


# ------------------ Variable and Subset Definitions -----------------------

#meta variables that are irrelevant for risk factor analysis
exclude_vars_base <- c(
  "farm_id", "total_samples", "positive_pigs",
  "percent_positive_pigs", "max_ct", "min_ct", "mean_ct", "sd_ct",
  "date_sampling"
)
#include to include if the associated general variable is significant 
overview_dependent_vars <- c(
  "quarantine_time", "outside_area_ai_centre", "outside_area_gestation_stable",
  "outside_area_farrowing_stable", "outside_area_weaner_stable", "outside_area_fattening_stable"
)
make_exclude_vars <- function(outcome_var) unique(c(exclude_vars_base, overview_dependent_vars, outcome_var))

get_husbandry_vars <- function(exclude_vars) setdiff(
  c("canton_factor", "herdsize", "production_type_factor",
    "Farrowing_on_farm", "Isemination_on_farm", "Gestation_on_farm", "Weaners_on_farm", "Fattening_on_farm",
    "horses_closeby", "dogs_closeby", "chicken_closeby", "turkey_closeby",
    "cattle_closeby", "cats_closeby", "proximity_to_other_pig_herd", "proximity_to_other_poultry_herd",
    "number_suckling_piglets", "number_weaners", "number_fattening_pigs", "number_young_sows",
    "number_old_sows", "number_boars", "number_of_origins", "quarantine_concept",
    "quarantine_in_herd_contact", "herds_of_origin_respiratory_symptoms", "herds_of_origin_influenza_diagnosis",
    "production_cycle", "mode_stable_occupation_ai_centre", "mode_stable_occupation_gestation_stable",
    "mode_stable_occupation_farrowing_stable", "cross_fostering_farrowing_stable",
    "mode_stable_occupation_weaner_stable", "mode_stable_occupation_fattening_stable",
    "passing_through_other_age_group", "outside_area",
    "outside_area_contact_poultry", "outside_area_contact_wild_birds", "outside_area_contact_wild_boars",
    "contact_bird_in_stable", "cleaning_ai_centre", "cleaning_gestation_stable", "cleaning_farrowing_stable",
    "cleaning_weaner_stable", "cleaning_fattening_stable", "cleaning_quarantine", "disinfection_ai_centre",
    "disinfection_gestation_stable", "disinfection_farrowing_stable", "disinfection_weaner_stable",
    "disinfection_fattening_stable", "disinfection_quarantine", "drying_ai_centre", "drying_gestation_stable",
    "drying_farrowing_stable", "drying_weaner_stable", "drying_fattening_stable", "drying_quarantine",
    "cleaning_disinfection_transport_vehicle", "cleaning_shipment_area", "caretaker_number",
    "caretaker_ppe_stable", "caretaker_ppe_washing_interval", "caretaker_ppe_per_unit", "caretaker_per_unit",
    "caretaker_work_flow_hygiene_between_units", "caretaker_entry_ppe_only", "caretaker_disease_management",
    "caretaker_hands_washed_before_entry", "caretaker_boot_disinfection", "caretaker_contact_other_pigs",
    "caretaker_contact_poultry", "visitors_cumulative_contact_hours",
    "visitors_list", "ppe_visitors", "visitors_hands_washed_before_entry", "visitors_disease_management",
    "visitors_contact_other_pigs", "visitors_respiratory_symptoms", "separation_quarantine_area", "bird_nests",
    "farrowing_airspace_with_other_agegroup","ai_airspace_with_other_agegroup",
    "gestation_sows_airspace_with_other_agegroup", "weaners_airspace_with_other_agegroup",
    "fattening_pigs_airspace_with_other_agegroup", "vet_consultation", "influenza_diagnosis", "influenza_vaccination"),
  exclude_vars)
get_animal_vars <- function(exclude_vars) setdiff(
  c("Farrowing_on_farm", "Isemination_on_farm", "Gestation_on_farm", "Weaners_on_farm", "Fattening_on_farm", "respiratory_signs", "return_to_service_rate", "farrowing_rate", "piglets_per_sow_year",
    "abortions_per_sow_year", "piglet_mortality", "feed_conversion_rate_fattening_pigs",
    "respiratory_history_swine", "time_respiratory_disease", "frequency_respi_outbreak",
   "outbreak_since_examination", "suckling_piglets_diseased",
    "weaners_diseased", "fattening_pigs_diseased", "young_sows_diseased", "old_sows_diseased",
    "boars_diseased", "report_killed_weaners",
    "report_killed_fattening_pigs", "report_killed_young_sows", "report_killed_old_sows",
    "symptom_swine_sneezing", "symptom_swine_coughing", "symptom_swine_nasal_discharge",
    "symptom_swine_fever", "symptom_swine_feed_intake_red", "symptom_swine_apathy",
    "symptom_swine_dyspnoea", "Age_weeks_factor",
    "farrowing_piglets_reduced_general_wellbeing", "farrowing_piglets_sneezing",
    "farrowing_piglets_coughing", "weaners_reduced_general_wellbeing",
    "weaners_sneezing", "weaners_coughing", "weaners_discharge", "rectal_temperature_max","rectal_temperature_avg",
    "fattening_pigs_reduced_general_wellbeing", "fattening_pigs_sneezing",
    "fattening_pigs_coughing"),
  exclude_vars)
get_environment_vars <- function(exclude_vars) setdiff(
  c("Farrowing_on_farm", "Isemination_on_farm", "Gestation_on_farm", "Weaners_on_farm", "Fattening_on_farm", "season_sampling","farrowing_room_temperature", "farrowing_nest_temperature_ok", "farrowing_airflow",
    "farrowing_air_quality", "ai_sows_room_temperature", "ai_sows_airflow", "ai_sows_air_quality",
    "gestation_sows_qm_per_animal", "gestation_sows_animals_per_water_source", "gestation_sows_room_temperature",
    "gestation_sows_airflow", "gestation_sows_air_quality", "weaners_qm_per_animal", "weaners_animals_per_feeding_site_factor",
    "weaners_animals_per_water_source", "weaners_room_temperature", "weaners_airflow",
    "weaners_air_quality", "fattening_pigs_qm_per_animal", "fattening_pigs_feeding_site_per_animal_factor",
    "fattening_pigs_animals_per_water_source", "fattening_pigs_room_temperature", "fattening_pigs_airflow",
    "fattening_pigs_air_quality"),
  exclude_vars)
get_human_vars <- function(exclude_vars) setdiff(
  c("respiratory_history_human", "season_sampling", "respiratory_history_contact_person",
    "symptom_human_sneezing", "symptom_human_coughing", "symptom_human_bronchitis",
    "symptom_human_pneumonia", "symptom_human_fever", "symptom_human_headache",
    "symptom_human_myalgia", "symptom_severity", "physician_consultation", "flu_vaccination",
    "flu_vaccination_contacts", "chronic_disease_condition", "smoker"),
  exclude_vars)

# ----- Custom statistical test assignment for continuous vars -------
wilcox_vars <- c(
  "number_fattening_pigs","number_boars","number_of_origins",
  "production_cycle","caretaker_number","visitors_cumulative_contact_hours","farrowing_airflow","ai_sows_airflow",
  "gestation_sows_qm_per_animal","gestation_sows_airflow",
  "weaners_sneezing","weaners_coughing","rectal_temperature_max","rectal_temperature_avg","weaners_qm_per_animal",
  "weaners_animals_per_feeding_site_factor","weaners_animals_per_water_source","weaners_airflow","weaners_air_quality","weaners_airspace_with_other_agegroup",
  "fattening_pigs_reduced_general_wellbeing","fattening_pigs_sneezing","fattening_pigs_coughing","fattening_pigs_discharge","fattening_pigs_airflow","fattening_pigs_air_quality","fattening_pigs_airspace_with_other_agegroup",
  "fattening_pigs_feeding_site_per_animal_factor",
  "farrowing_piglets_sneezing",
  "farrowing_piglets_coughing"
)
ttest_vars <- c(
  "herdsize","number_suckling_piglets","number_weaners","number_young_sows","number_old_sows",
  "gestation_sows_animals_per_water_source","gestation_sows_room_temperature",
  "weaners_room_temperature","fattening_pigs_qm_per_animal",
  "fattening_pigs_animals_per_water_source","fattening_pigs_room_temperature",
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
  if (var_name == "fattening_pigs_qm_per_animal") idx <- x <= 4
  if (var_name == "fattening_pigs_animals_per_water_source") idx <- x <= 30
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
  y <- df[["IAV_positive"]]
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
          paste0(subset_name, ": Correlation with IAV_positive (All variables)")
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
outcome_var <- "IAV_positive"
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
    tags$h2("Univariate Association with IAV_positive by Subset"),
    dt_husbandry, tags$hr(),
    dt_animals, tags$hr(),
    dt_environment, tags$hr(),
    dt_human
  ),
  file = "04_output/association_IAV_positive_by_subset.html"
)

# Correlation table for ALL variables in each subset (not just significant)
corr_husbandry   <- make_correlation_results(results_husbandry,   "Husbandry",   df3)
corr_animals     <- make_correlation_results(results_animals,     "Animals",     df3)
corr_environment <- make_correlation_results(results_environment, "Environment", df3)
corr_human       <- make_correlation_results(results_human,       "Human",       df3)

save_html(
  tagList(
    tags$h2("Correlation of All Variables with IAV_positive by Subset"),
    if (!is.null(corr_husbandry))   make_corr_dt(corr_husbandry,   "Husbandry")   else tags$p("No variables for Husbandry."),
    tags$hr(),
    if (!is.null(corr_animals))     make_corr_dt(corr_animals,     "Animals")     else tags$p("No variables for Animals."),
    tags$hr(),
    if (!is.null(corr_environment)) make_corr_dt(corr_environment, "Environment") else tags$p("No variables for Environment."),
    tags$hr(),
    if (!is.null(corr_human))       make_corr_dt(corr_human,       "Human")       else tags$p("No variables for Human.")
  ),
  file = "04_output/correlation_IAV_positive_by_subset.html"
)





# Create a new workbook
wb <- createWorkbook()

# ================= Association Tables =================
assoc_list <- list(
  Husbandry   = results_husbandry,
  Animals     = results_animals,
  Environment = results_environment,
  Human       = results_human
)

# Add each association table as a sheet
for (subset_name in names(assoc_list)) {
  df <- assoc_list[[subset_name]] %>%
    mutate(pval = signif(pval, 3),
           pval_fdr = signif(pval_fdr, 3))
  addWorksheet(wb, paste0("Assoc_", subset_name))
  writeData(wb, sheet = paste0("Assoc_", subset_name), df)
}

# ================= Correlation Tables =================
corr_list <- list(
  Husbandry   = corr_husbandry,
  Animals     = corr_animals,
  Environment = corr_environment,
  Human       = corr_human
)

# Add each correlation table as a sheet
for (subset_name in names(corr_list)) {
  df <- corr_list[[subset_name]] %>%
    mutate(pval = signif(pval, 3),
           pval_fdr = signif(pval_fdr, 3),
           coef = signif(coef, 3))
  addWorksheet(wb, paste0("Corr_", subset_name))
  writeData(wb, sheet = paste0("Corr_", subset_name), df)
}

# ================= Save Workbook =================
saveWorkbook(wb, file = "04_output/IAV_positive_Assoc_Corr.xlsx", overwrite = TRUE)


# =========================
# Correlation matrices by subset and overall (Excel output)
# =========================
# Produces five sets of matrices (coefficients, p-values, methods, N):
# - Husbandry subset
# - Animals subset
# - Environment subset
# - Human subset
# - All variables in df3 (after exclusions)
#
# Methods per pair:
# - Numeric vs numeric: Pearson (default). Set numeric_method = "spearman" to switch.
# - Any pair involving a binary variable (0/1, TRUE/FALSE, 2-level factor): Spearman by default
#   (point-biserial = Pearson on 0/1 is also reasonable; set binary_method = "pearson" if preferred).
# - Categorical vs categorical (factors/logicals with >=2 levels): Cramér's V (p from chi-squared).
# - Numeric vs multi-level categorical: correlation ratio (eta) with p from one-way ANOVA (F-test).
#
# Output: 04_output/IAV_positive_Correlation_Matrices.xlsx
# Sheets per subset: Corr_<Subset>_Coeff, Corr_<Subset>_p, Corr_<Subset>_Method, Corr_<Subset>_N
# Also adds a "Notes" sheet documenting method selection.

suppressPackageStartupMessages({
  library(dplyr)
  library(purrr)
  library(openxlsx)
})

# ---------- Type helpers ----------
is_binary_var <- function(x) {
  if (is.logical(x)) return(TRUE)
  if (is.factor(x)) return(length(levels(x)) == 2)
  if (is.numeric(x)) {
    ux <- unique(x[!is.na(x)])
    return(length(ux) <= 2)
  }
  FALSE
}
is_categorical_var <- function(x) {
  is.factor(x) || is.logical(x)
}
is_multi_cat <- function(x) is.factor(x) && length(levels(x)) > 2
is_numeric_var <- function(x) is.numeric(x)

# ---------- Core measures ----------
safe_cor_test <- function(x, y, method = "pearson") {
  # x,y numeric with complete cases
  idx <- is.finite(x) & is.finite(y)
  x <- x[idx]; y <- y[idx]
  n <- length(x)
  if (n < 3) return(list(estimate = NA_real_, p.value = NA_real_, n = n))
  ct <- tryCatch(suppressWarnings(cor.test(x, y, method = method)),
                 error = function(e) NULL)
  if (is.null(ct)) return(list(estimate = NA_real_, p.value = NA_real_, n = n))
  list(estimate = as.numeric(ct$estimate), p.value = as.numeric(ct$p.value), n = n)
}

cramers_v_test <- function(x, y) {
  # x,y categorical (factor/logical/binary)
  xx <- if (is.logical(x)) factor(x) else if (is.factor(x)) x else factor(x)
  yy <- if (is.logical(y)) factor(y) else if (is.factor(y)) y else factor(y)
  idx <- complete.cases(xx, yy)
  xx <- droplevels(xx[idx]); yy <- droplevels(yy[idx])
  n <- length(xx)
  if (n < 3) return(list(V = NA_real_, p.value = NA_real_, n = n))
  tbl <- table(xx, yy)
  if (any(dim(tbl) < 2) || sum(tbl) == 0) return(list(V = NA_real_, p.value = NA_real_, n = n))
  chi2 <- tryCatch(suppressWarnings(chisq.test(tbl, correct = FALSE)), error = function(e) NULL)
  if (is.null(chi2)) return(list(V = NA_real_, p.value = NA_real_, n = n))
  k <- min(nrow(tbl), ncol(tbl))
  V <- sqrt(as.numeric(chi2$statistic) / (n * (k - 1)))
  list(V = V, p.value = as.numeric(chi2$p.value), n = n)
}

eta_ratio_test <- function(x_num, y_cat) {
  # Correlation ratio (eta) for numeric vs multi-level categorical
  idx <- is.finite(x_num) & !is.na(y_cat)
  x <- x_num[idx]
  y <- droplevels(as.factor(y_cat[idx]))
  n <- length(x)
  if (n < 3 || length(levels(y)) < 2) return(list(eta = NA_real_, p.value = NA_real_, n = n))
  # Between-group sum of squares
  grand_mean <- mean(x)
  groups <- split(x, y)
  n_g <- sapply(groups, length)
  mean_g <- sapply(groups, mean)
  ss_between <- sum(n_g * (mean_g - grand_mean)^2)
  ss_total <- sum((x - grand_mean)^2)
  eta <- if (ss_total > 0) sqrt(ss_between / ss_total) else NA_real_
  # ANOVA F-test p-value
  fit <- tryCatch(anova(lm(x ~ y)), error = function(e) NULL)
  pval <- if (is.null(fit)) NA_real_ else as.numeric(fit[["Pr(>F)"]][1])
  list(eta = eta, p.value = pval, n = n)
}

# ---------- Pairwise selector ----------
pairwise_assoc <- function(x, y,
                           numeric_method = "pearson",
                           binary_method = "spearman") {
  # Decide method based on types
  x_is_num <- is_numeric_var(x)
  y_is_num <- is_numeric_var(y)
  x_is_bin <- is_binary_var(x)
  y_is_bin <- is_binary_var(y)
  x_is_cat <- is_categorical_var(x)
  y_is_cat <- is_categorical_var(y)
  x_is_multi <- is_multi_cat(x)
  y_is_multi <- is_multi_cat(y)
  
  # Categorical vs categorical -> Cramér's V
  if (x_is_cat && y_is_cat) {
    res <- cramers_v_test(x, y)
    return(list(coef = res$V, p = res$p.value, n = res$n, method = "Cramér's V"))
  }
  
  # Numeric vs multi-level categorical -> eta
  if ((x_is_num && y_is_multi) || (y_is_num && x_is_multi)) {
    if (x_is_num) {
      res <- eta_ratio_test(x, y)
    } else {
      res <- eta_ratio_test(y, x)
    }
    return(list(coef = res$eta, p = res$p.value, n = res$n, method = "Eta (ANOVA)"))
  }
  
  # Any pair involving binary -> Spearman (or Pearson if requested)
  if (x_is_bin || y_is_bin) {
    x_num <- if (is.numeric(x)) x else as.numeric(as.factor(x)) - 1
    y_num <- if (is.numeric(y)) y else as.numeric(as.factor(y)) - 1
    method <- match.arg(binary_method, c("spearman", "pearson"))
    res <- safe_cor_test(x_num, y_num, method = method)
    mname <- if (method == "spearman") "Spearman (binary)" else "Pearson (binary)"
    return(list(coef = res$estimate, p = res$p.value, n = res$n, method = mname))
  }
  
  # Numeric vs numeric -> Pearson or Spearman
  if (x_is_num && y_is_num) {
    method <- match.arg(numeric_method, c("pearson", "spearman"))
    res <- safe_cor_test(x, y, method = method)
    mname <- if (method == "pearson") "Pearson" else "Spearman"
    return(list(coef = res$estimate, p = res$p.value, n = res$n, method = mname))
  }
  
  # Fallback: try Spearman on numeric-coded values
  x_num <- suppressWarnings(as.numeric(x))
  y_num <- suppressWarnings(as.numeric(y))
  res <- safe_cor_test(x_num, y_num, method = "spearman")
  return(list(coef = res$estimate, p = res$p.value, n = res$n, method = "Spearman (fallback)"))
}

# ---------- Build matrices ----------
build_corr_matrices <- function(df, var_names,
                                numeric_method = "pearson",
                                binary_method = "spearman") {
  vars <- var_names[var_names %in% names(df)]
  p <- length(vars)
  if (p < 2) stop("Not enough variables to build a correlation matrix.")
  
  mat_coef   <- matrix(NA_real_, nrow = p, ncol = p, dimnames = list(vars, vars))
  mat_p      <- matrix(NA_real_, nrow = p, ncol = p, dimnames = list(vars, vars))
  mat_n      <- matrix(NA_integer_, nrow = p, ncol = p, dimnames = list(vars, vars))
  mat_method <- matrix("", nrow = p, ncol = p, dimnames = list(vars, vars))
  
  for (i in seq_len(p)) {
    for (j in seq_len(p)) {
      xi <- df[[vars[i]]]
      yj <- df[[vars[j]]]
      if (i == j) {
        mat_coef[i, j] <- 1
        mat_p[i, j] <- NA
        mat_n[i, j] <- sum(complete.cases(xi))
        mat_method[i, j] <- "-"
      } else {
        res <- pairwise_assoc(xi, yj, numeric_method = numeric_method, binary_method = binary_method)
        mat_coef[i, j]   <- res$coef
        mat_p[i, j]      <- res$p
        mat_n[i, j]      <- res$n
        mat_method[i, j] <- res$method
      }
    }
  }
  list(coef = mat_coef, p = mat_p, n = mat_n, method = mat_method)
}

# ---------- Write matrices to workbook ----------
write_corr_matrices <- function(wb, prefix, matrices) {
  addWorksheet(wb, paste0("Corr_", prefix, "_Coeff"))
  writeData(wb, paste0("Corr_", prefix, "_Coeff"), as.data.frame(matrices$coef), rowNames = TRUE)
  addWorksheet(wb, paste0("Corr_", prefix, "_p"))
  writeData(wb, paste0("Corr_", prefix, "_p"), as.data.frame(matrices$p), rowNames = TRUE)
  addWorksheet(wb, paste0("Corr_", prefix, "_N"))
  writeData(wb, paste0("Corr_", prefix, "_N"), as.data.frame(matrices$n), rowNames = TRUE)
  addWorksheet(wb, paste0("Corr_", prefix, "_Method"))
  writeData(wb, paste0("Corr_", prefix, "_Method"), as.data.frame(matrices$method), rowNames = TRUE)
}

# ---------- Prepare subsets ----------
outcome_var <- "IAV_positive"
exclude_vars <- make_exclude_vars(outcome_var)

husbandry_vars   <- get_husbandry_vars(exclude_vars)
animal_vars      <- get_animal_vars(exclude_vars)
environment_vars <- get_environment_vars(exclude_vars)
human_vars       <- get_human_vars(exclude_vars)

# Optionally filter out variables that are entirely NA or constant
drop_all_na_or_constant <- function(df, var_names) {
  var_names[sapply(var_names, function(v) {
    x <- df[[v]]
    if (is.null(x)) return(FALSE)
    nn <- sum(!is.na(x))
    if (nn < 3) return(FALSE)
    ux <- unique(x[!is.na(x)])
    length(ux) > 1
  })]
}

husbandry_vars_f   <- drop_all_na_or_constant(df3, husbandry_vars)
animal_vars_f      <- drop_all_na_or_constant(df3, animal_vars)
environment_vars_f <- drop_all_na_or_constant(df3, environment_vars)
human_vars_f       <- drop_all_na_or_constant(df3, human_vars)

# ---------- Build and save workbook ----------
if (!dir.exists("04_output")) dir.create("04_output", recursive = TRUE)
wb <- createWorkbook()

# Notes sheet describing methods
addWorksheet(wb, "Notes")
writeData(wb, "Notes",
          "Correlation matrices method selection:
- Numeric vs numeric: Pearson (default). Set numeric_method = 'spearman' to use Spearman.
- Any pair involving a binary variable (0/1, TRUE/FALSE, 2-level factor): Spearman by default (set binary_method = 'pearson' if preferred).
- Categorical vs categorical: Cramér's V (p-value from chi-squared test).
- Numeric vs multi-level categorical: correlation ratio (eta) with p-value from one-way ANOVA (F-test).
Matrices include coefficient, p-value, method used, and N (complete cases).")

# Build matrices per subset (you can change numeric_method/binary_method if desired)
mat_husbandry   <- build_corr_matrices(df3, husbandry_vars_f,   numeric_method = "pearson", binary_method = "spearman")
mat_animals     <- build_corr_matrices(df3, animal_vars_f,      numeric_method = "pearson", binary_method = "spearman")
mat_environment <- build_corr_matrices(df3, environment_vars_f, numeric_method = "pearson", binary_method = "spearman")
mat_human       <- build_corr_matrices(df3, human_vars_f,       numeric_method = "pearson", binary_method = "spearman")

write_corr_matrices(wb, "Husbandry",   mat_husbandry)
write_corr_matrices(wb, "Animals",     mat_animals)
write_corr_matrices(wb, "Environment", mat_environment)
write_corr_matrices(wb, "Human",       mat_human)

# Overall matrix for all variables (after exclusions)
all_vars <- c(husbandry_vars_f, animal_vars_f, environment_vars_f, human_vars_f)
all_vars <- unique(all_vars)
mat_all <- build_corr_matrices(df3, all_vars, numeric_method = "pearson", binary_method = "spearman")
write_corr_matrices(wb, "All", mat_all)

# Save workbook
saveWorkbook(wb, file = "04_output/IAV_positive_Correlation_Matrices.xlsx", overwrite = TRUE)
cat("Correlation matrices written to 04_output/IAV_positive_Correlation_Matrices.xlsx\n")

# ================= All variables in df3 (true full matrix) =================
# Toggle: set include_excluded <- TRUE to literally include every column in df3.
include_excluded <- FALSE

if (include_excluded) {
  all_vars_df3 <- names(df3)
} else {
  # Exclude meta/outcome vars (farm_id, totals, CTs, dates, outcome, etc.)
  # exclude_vars was defined earlier via make_exclude_vars(outcome_var)
  all_vars_df3 <- setdiff(names(df3), exclude_vars)
}

# Drop vars that are all NA or constant, and require at least 3 non-missing
all_vars_df3 <- drop_all_na_or_constant(df3, all_vars_df3)

# Optional safety: if extremely wide, you can limit to top K variables
# K <- 200; if (length(all_vars_df3) > K) all_vars_df3 <- all_vars_df3[seq_len(K)]

mat_all_df3 <- build_corr_matrices(
  df3,
  all_vars_df3,
  numeric_method = "pearson",
  binary_method  = "spearman"
)

write_corr_matrices(wb, "All_df3", mat_all_df3)

saveWorkbook(wb, file = "04_output/IAV_positive_Correlation_Matrices.xlsx", overwrite = TRUE)
cat("Correlation matrices written to 04_output/IAV_positive_Correlation_Matrices.xlsx\n")

# ================= Thresholded pairs (|association| >= 0.70) from full df3 matrix =================
# Reuses: df3, exclude_vars, drop_all_na_or_constant(df, vars), build_corr_matrices()
# Output: 04_output/All_df3_Correlation_Pairs_abs_gte_070.xlsx

corr_threshold <- 0.70
include_excluded <- FALSE  # set TRUE to include literally every df3 column (no exclusions)

# Build the full "All_df3" matrices once if not already present
if (!exists("mat_all_df3")) {
  all_vars_df3 <- if (include_excluded) names(df3) else setdiff(names(df3), exclude_vars)
  all_vars_df3 <- drop_all_na_or_constant(df3, all_vars_df3)
  mat_all_df3 <- build_corr_matrices(
    df3,
    all_vars_df3,
    numeric_method = "pearson",
    binary_method  = "spearman"
  )
}

# Flatten to all unique pairs (upper triangle), then filter by threshold
vars <- colnames(mat_all_df3$coef)
idx_upper <- which(upper.tri(mat_all_df3$coef), arr.ind = TRUE)

pairs_thresh <- data.frame(
  var1     = vars[idx_upper[, 1]],
  var2     = vars[idx_upper[, 2]],
  coef     = as.numeric(mat_all_df3$coef[idx_upper]),
  abs_coef = abs(as.numeric(mat_all_df3$coef[idx_upper])),
  p_value  = as.numeric(mat_all_df3$p[idx_upper]),
  N        = as.integer(mat_all_df3$n[idx_upper]),
  method   = as.character(mat_all_df3$method[idx_upper]),
  stringsAsFactors = FALSE
) |>
  dplyr::filter(is.finite(abs_coef), abs_coef >= corr_threshold) |>
  dplyr::mutate(
    coef     = round(coef, 4),
    abs_coef = round(abs_coef, 4),
    p_value  = ifelse(is.na(p_value), "", formatC(p_value, digits = 4, format = "g"))
  ) |>
  dplyr::arrange(dplyr::desc(abs_coef), var1, var2)

# Safe filename/sheet name (avoid '>' which is invalid on Windows)
if (!dir.exists("04_output")) dir.create("04_output", recursive = TRUE)
threshold_tag <- gsub("\\.", "", sprintf("%.2f", corr_threshold))  # 0.70 -> "070"
sheet_name    <- sprintf("Pairs_abs_gte_%s", threshold_tag)        # e.g., "Pairs_abs_gte_070"
out_file      <- sprintf("04_output/All_df3_Correlation_Pairs_threshold_07.xlsx", threshold_tag)

wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, sheet_name)
if (nrow(pairs_thresh) == 0) {
  openxlsx::writeData(wb, sheet_name, data.frame(message = paste0("No pairs with |association| >= ", corr_threshold)))
} else {
  openxlsx::writeData(wb, sheet_name, pairs_thresh)
}

openxlsx::addWorksheet(wb, "Notes")
openxlsx::writeData(wb, "Notes", c(
  paste0("All unique df3 pairs with |association| >= ", corr_threshold, "."),
  "Methods per pair: Pearson (num-num), Spearman (if any binary), Cramér's V (cat-cat), Eta (num vs multi-cat).",
  "Columns: var1, var2, coef, abs_coef, p_value, N, method.",
  paste0("Variables in All_df3 matrix: ", length(vars))
))

openxlsx::saveWorkbook(wb, file = out_file, overwrite = TRUE)
cat("Thresholded df3 correlation pairs written to: ", normalizePath(out_file, winslash = "/"), "\n")

# =====================================================================
# OUTCOME SUMMARY + GROUP COMPARISON TESTS (IAV_positive)
# =====================================================================
# This block produces:
#  1) Outcome summary table (dt_outcome):
#     - % IAV-positive herds (overall) with 95% CI (Wilson score)
#     - % IAV-positive among respiratory TRUE / FALSE with 95% CI (Wilson score)
#     - Average herd-level prevalence (positive_pigs / total_samples) overall
#       and by respiratory TRUE / FALSE with 95% CI (percentile bootstrap)
#     - FINAL ROW: p-values for group differences (proportion + prevalence)
#     - CI columns combined into a single "95% CI" column
#     Saves to: 04_output/iav_outcome_summary.html
#
#  2) Group comparison tests table (dt_tests):
#     - IAV-positive proportions (resp TRUE vs FALSE): Fisher's exact or Chi-squared
#     - Mean herd-level prevalence (resp TRUE vs FALSE): Wilcoxon Mann–Whitney
#     Saves to: 04_output/iav_outcome_group_tests.html
#
#  3) Combined HTML with both tables:
#     Saves to: 04_output/iav_outcome_summary_and_tests.html
# =====================================================================

# ---- Robust helpers (type-tolerant) ----

# Map logical/0-1/factor/character ("positive"/"negative", "true"/"false") to 0/1/NA
coerce_binary01 <- function(x) {
  # Keep NA as NA
  if (is.logical(x)) return(ifelse(is.na(x), NA_integer_, as.integer(x)))
  if (is.numeric(x) || is.integer(x)) {
    return(ifelse(is.na(x), NA_integer_, ifelse(x == 1, 1L, ifelse(x == 0, 0L, NA_integer_))))
  }
  lx <- tolower(as.character(x))
  y <- ifelse(lx %in% c("1", "true", "positive", "yes"), 1L,
              ifelse(lx %in% c("0", "false", "negative", "no"), 0L, NA_integer_))
  return(y)
}

# Map logical/0-1/factor/character ("true"/"false", "1"/"0") to logical TRUE/FALSE/NA
coerce_flag_logical <- function(x) {
  if (is.logical(x)) return(x)
  if (is.numeric(x) || is.integer(x)) return(ifelse(is.na(x), NA, x == 1))
  lx <- tolower(as.character(x))
  out <- ifelse(lx %in% c("true", "1", "yes"), TRUE,
                ifelse(lx %in% c("false", "0", "no"), FALSE, NA))
  return(out)
}

# ---- Wilson proportion CI; bootstrap CI for mean; % formatter ----
compute_prop_ci <- function(x_any, conf.level = 0.95) {
  # Accepts logical/numeric/factor/character indicating positives
  y <- coerce_binary01(x_any)
  n <- sum(!is.na(y))
  k <- sum(y == 1L, na.rm = TRUE)
  if (n == 0) return(list(prop = NA_real_, lower = NA_real_, upper = NA_real_, n = 0, k = 0))
  
  p <- k / n
  z <- qnorm(1 - (1 - conf.level) / 2)
  
  denom  <- 1 + (z^2) / n
  center <- p + (z^2) / (2 * n)
  adj    <- z * sqrt(p * (1 - p) / n + (z^2) / (4 * n^2))
  
  lower <- max(0, (center - adj) / denom)
  upper <- min(1, (center + adj) / denom)
  
  list(prop = p, lower = lower, upper = upper, n = n, k = k)
}

compute_bootstrap_mean_ci <- function(x, B = 10000, seed = 20260123) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n == 0) return(list(mean = NA_real_, lower = NA_real_, upper = NA_real_, n = 0))
  set.seed(seed)
  boot_means <- replicate(B, mean(sample(x, replace = TRUE)))
  ci <- stats::quantile(boot_means, probs = c(0.025, 0.975), names = FALSE)
  list(mean = mean(x), lower = ci[1], upper = ci[2], n = n)
}

to_pct <- function(x) ifelse(is.na(x), NA_character_, sprintf("%.1f", 100 * x))

# Combine CI into a single string
format_ci <- function(lo, hi) {
  if (is.na(lo) || is.na(hi)) return("")
  sprintf("[%s, %s]", to_pct(lo), to_pct(hi))
}

# ---- Detect respiratory variable name in df3 (then coerce robustly) ----
resp_var <- if ("respiratory_signs" %in% names(df3)) {
  "respiratory_signs"
} else if ("respiratory_symptoms" %in% names(df3)) {
  "respiratory_symptoms"
} else {
  stop("Neither 'respiratory_signs' nor 'respiratory_symptoms' found in df3.")
}
resp_flag_log <- coerce_flag_logical(df3[[resp_var]])

# =========================
# 1) OUTCOME SUMMARY TABLE (base rows)
# =========================

# 1a. Herd-level positivity (% IAV-positive) overall and by respiratory subgroup (Wilson CI)
overall_ci <- compute_prop_ci(df3$IAV_positive)

idx_true  <- resp_flag_log == TRUE
idx_false <- resp_flag_log == FALSE
rs_true_ci  <- compute_prop_ci(df3$IAV_positive[idx_true])
rs_false_ci <- compute_prop_ci(df3$IAV_positive[idx_false])

# 1b. Per-herd prevalence (positive_pigs / total_samples), overall and by subgroup (bootstrap CI)
if (!exists("prev_df")) {
  prev_df <- df3 %>%
    dplyr::filter(!is.na(total_samples), total_samples > 0, !is.na(positive_pigs)) %>%
    dplyr::mutate(per_herd_prev = positive_pigs / total_samples)
}

overall_prev <- compute_bootstrap_mean_ci(prev_df$per_herd_prev)

prev_true_df  <- prev_df[resp_flag_log[match(prev_df$farm_id, df3$farm_id)] == TRUE, , drop = FALSE]
prev_false_df <- prev_df[resp_flag_log[match(prev_df$farm_id, df3$farm_id)] == FALSE, , drop = FALSE]
rs_true_prev  <- compute_bootstrap_mean_ci(prev_true_df$per_herd_prev)
rs_false_prev <- compute_bootstrap_mean_ci(prev_false_df$per_herd_prev)

# Build base summary (without the final p-value row yet)
outcome_summary_base <- dplyr::tibble(
  Metric = c(
    "IAV positive (all herds)",
    "IAV positive (herds with respiratory signs)",
    "IAV positive (herds without respiratory signs)",
    "Average herd-level IAV prevalence (all herds)",
    "Average herd-level IAV prevalence (with respiratory signs)",
    "Average herd-level IAV prevalence (without respiratory signs)"
  ),
  Estimate_percent = c(
    to_pct(overall_ci$prop),
    to_pct(rs_true_ci$prop),
    to_pct(rs_false_ci$prop),
    to_pct(overall_prev$mean),
    to_pct(rs_true_prev$mean),
    to_pct(rs_false_prev$mean)
  ),
  CI_95 = c(
    format_ci(overall_ci$lower, overall_ci$upper),
    format_ci(rs_true_ci$lower, rs_true_ci$upper),
    format_ci(rs_false_ci$lower, rs_false_ci$upper),
    format_ci(overall_prev$lower, overall_prev$upper),
    format_ci(rs_true_prev$lower, rs_true_prev$upper),
    format_ci(rs_false_prev$lower, rs_false_prev$upper)
  ),
  N_herds = c(
    overall_ci$n,
    rs_true_ci$n,
    rs_false_ci$n,
    overall_prev$n,
    rs_true_prev$n,
    rs_false_prev$n
  ),
  IAV_positive_herds = c(
    overall_ci$k,
    rs_true_ci$k,
    rs_false_ci$k,
    NA_integer_,
    NA_integer_,
    NA_integer_
  ),
  p_value_group_diff = ""  # filled only in the appended final row
)

# ==================================
# 2) GROUP COMPARISON TESTS TABLE
# ==================================

# Build a clean 2x2 dataset for proportion test
df_bin <- data.frame(
  resp_flag = resp_flag_log,
  iav_01    = coerce_binary01(df3$IAV_positive)
)
df_bin <- df_bin[!is.na(df_bin$resp_flag) & !is.na(df_bin$iav_01), , drop = FALSE]

# 2a. Proportion test: IAV-positive herds by respiratory TRUE/FALSE
tbl <- table(df_bin$resp_flag, factor(df_bin$iav_01, levels = c(0,1), labels = c("negative","positive")))
if (any(dim(tbl) < 2) || sum(tbl) == 0) {
  prop_test_type <- "Not analyzable"
  prop_test_p <- NA_real_
} else {
  expected <- outer(rowSums(tbl), colSums(tbl)) / sum(tbl)
  prop_test_type <- if (any(expected < 5, na.rm = TRUE)) "Fisher's exact" else "Chi-squared"
  prop_test_p <- tryCatch(
    {
      if (prop_test_type == "Fisher's exact") fisher.test(tbl)$p.value else chisq.test(tbl, correct = FALSE)$p.value
    },
    error = function(e) NA_real_
  )
}

prop_true  <- if (any(df_bin$resp_flag == TRUE))  mean(df_bin$iav_01[df_bin$resp_flag == TRUE])  else NA_real_
prop_false <- if (any(df_bin$resp_flag == FALSE)) mean(df_bin$iav_01[df_bin$resp_flag == FALSE]) else NA_real_
prop_diff  <- if (is.finite(prop_true) && is.finite(prop_false)) prop_true - prop_false else NA_real_
n_true     <- sum(df_bin$resp_flag == TRUE)
n_false    <- sum(df_bin$resp_flag == FALSE)

# 2b. Prevalence test: per-herd prevalence by respiratory TRUE/FALSE (Wilcoxon)
prev_df_groups <- data.frame(
  per_herd_prev = prev_df$per_herd_prev,
  resp_flag = resp_flag_log[match(prev_df$farm_id, df3$farm_id)]
)
prev_df_groups <- prev_df_groups[!is.na(prev_df_groups$resp_flag), , drop = FALSE]

mean_true_prev  <- with(prev_df_groups, mean(per_herd_prev[resp_flag == TRUE],  na.rm = TRUE))
mean_false_prev <- with(prev_df_groups, mean(per_herd_prev[resp_flag == FALSE], na.rm = TRUE))
n_true_prev     <- sum(prev_df_groups$resp_flag == TRUE,  na.rm = TRUE)
n_false_prev    <- sum(prev_df_groups$resp_flag == FALSE, na.rm = TRUE)

prev_test_p <- tryCatch(
  {
    if (min(n_true_prev, n_false_prev) >= 1) {
      wilcox.test(per_herd_prev ~ resp_flag, data = prev_df_groups, exact = FALSE)$p.value
    } else NA_real_
  },
  error = function(e) NA_real_
)
prev_diff <- mean_true_prev - mean_false_prev

tests_summary <- dplyr::tibble(
  Comparison = c(
    "IAV-positive herd proportion (TRUE vs FALSE)",
    "Average herd-level IAV prevalence (TRUE vs FALSE)"
  ),
  Group_TRUE_Estimate_percent  = c(to_pct(prop_true),  to_pct(mean_true_prev)),
  Group_FALSE_Estimate_percent = c(to_pct(prop_false), to_pct(mean_false_prev)),
  Difference_percent           = c(to_pct(prop_diff),  to_pct(prev_diff)),
  Test                         = c(prop_test_type,     "Wilcoxon Mann–Whitney"),
  p_value                      = c(signif(prop_test_p, 3), signif(prev_test_p, 3)),
  N_TRUE                       = c(n_true,             n_true_prev),
  N_FALSE                      = c(n_false,            n_false_prev)
)

dt_outcome_tests_caption <- htmltools::tags$caption(
  style = 'caption-side: top; text-align: left;',
  htmltools::tags$strong("Group Comparison Tests: respiratory TRUE vs FALSE"),
  htmltools::tags$br(),
  htmltools::tags$span(
    style = "font-size: 0.95em; color: #555;",
    "Proportion test: Fisher's exact when expected cell count < 5; otherwise Chi-squared.",
    htmltools::tags$br(),
    "Prevalence test: Wilcoxon Mann–Whitney on per-herd prevalence (positive_pigs / total_samples).",
    htmltools::tags$br(),
    "Estimates shown as percent; difference = TRUE minus FALSE."
  )
)

dt_tests <- DT::datatable(
  tests_summary,
  caption = dt_outcome_tests_caption,
  rownames = FALSE,
  options = list(pageLength = 10, autoWidth = TRUE),
  colnames = c(
    "Comparison", "TRUE estimate (%)", "FALSE estimate (%)", "Difference (%)",
    "Test", "p-Value", "N TRUE", "N FALSE"
  )
)

htmlwidgets::saveWidget(
  widget = dt_tests,
  file = "04_output/iav_outcome_group_tests.html",
  selfcontained = TRUE
)

# =========================
# 1c. FINALIZE OUTCOME SUMMARY (append p-values row and render)
# =========================

# Append a final row with group-difference p-values for convenience
p_summary_row <- dplyr::tibble(
  Metric = "Group difference p-values (resp TRUE vs FALSE)",
  Estimate_percent = NA_character_,
  CI_95 = "",
  N_herds = NA_integer_,
  IAV_positive_herds = NA_integer_,
  p_value_group_diff = sprintf("Proportion: %s | Prevalence: %s",
                               ifelse(is.na(prop_test_p), "", signif(prop_test_p, 3)),
                               ifelse(is.na(prev_test_p), "", signif(prev_test_p, 3)))
)

outcome_summary <- dplyr::bind_rows(outcome_summary_base, p_summary_row)

dt_outcome_caption <- htmltools::tags$caption(
  style = 'caption-side: top; text-align: left;',
  htmltools::tags$strong("Outcome Summary: IAV_positive"),
  htmltools::tags$br(),
  htmltools::tags$span(
    style = "font-size: 0.95em; color: #555;",
    "Herd-level positivity uses Wilson score 95% CIs.",
    htmltools::tags$br(),
    "Average herd-level prevalence is the mean of per-herd (positive_pigs / total_samples); CI via percentile bootstrap (B = 10,000).",
    htmltools::tags$br(),
    sprintf("Subgroup results shown for %s == TRUE and == FALSE. Final row reports p-values for group differences.", resp_var)
  )
)

dt_outcome <- DT::datatable(
  outcome_summary,
  caption = dt_outcome_caption,
  rownames = FALSE,
  options = list(pageLength = 10, autoWidth = TRUE),
  colnames = c("Metric", "Estimate (%)", "95% CI", "N herds", "IAV+ herds", "p-value (group diff)")
)

htmlwidgets::saveWidget(
  widget = dt_outcome,
  file = "04_output/iav_outcome_summary.html",
  selfcontained = TRUE
)

# =========================================
# 3) COMBINED HTML (OUTCOME + TESTS TABLE)
# =========================================
combined_file <- "04_output/iav_outcome_summary_and_tests.html"
htmltools::save_html(
  htmltools::tagList(
    htmltools::tags$h2("Outcome Summary: IAV_positive"),
    dt_outcome,
    htmltools::tags$hr(),
    htmltools::tags$h2("Group Comparison Tests: respiratory TRUE vs FALSE"),
    dt_tests
  ),
  file = combined_file
)
cat("Combined HTML written to:", combined_file, "\n")

# =========================================
# 4) EXPORT TO EXCEL
# =========================================
suppressPackageStartupMessages({
  if (!requireNamespace("openxlsx", quietly = TRUE)) {
    stop("Package 'openxlsx' is required. Please install it with install.packages('openxlsx').")
  }
  library(openxlsx)
})

# Ensure output directory exists
out_dir <- "04_output"
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

# Create workbook
wb <- createWorkbook()

# Write Outcome Summary (includes combined 95% CI column and final p-values row)
addWorksheet(wb, "Outcome_Summary")
writeData(wb, "Outcome_Summary", outcome_summary)
freezePane(wb, "Outcome_Summary", firstActiveRow = 2)
setColWidths(wb, "Outcome_Summary", cols = 1:ncol(outcome_summary), widths = "auto")

# Write Group Comparison Tests
addWorksheet(wb, "Group_Tests")
writeData(wb, "Group_Tests", tests_summary)
freezePane(wb, "Group_Tests", firstActiveRow = 2)
setColWidths(wb, "Group_Tests", cols = 1:ncol(tests_summary), widths = "auto")

# Save workbook
xlsx_path <- file.path(out_dir, "IAV_outcome_summary_and_tests.xlsx")
saveWorkbook(wb, file = xlsx_path, overwrite = TRUE)
cat("Excel workbook written to:", normalizePath(xlsx_path, winslash = "/"), "\n")


# Herd-level mean_ct comparison by respiratory signs using Welch t-test on means
# Standalone block: no dependency on find_var() or summarize_continuous_by_group_with_overall()

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

# Helper: coerce respiratory flag to logical TRUE/FALSE/NA
coerce_flag_logical <- function(x) {
  if (is.logical(x)) return(x)
  if (is.numeric(x) || is.integer(x)) return(ifelse(is.na(x), NA, x == 1))
  lx <- tolower(as.character(x))
  out <- ifelse(lx %in% c("true", "1", "yes"), TRUE,
                ifelse(lx %in% c("false", "0", "no"), FALSE, NA))
  return(out)
}

# Helper: mean and t-based 95% CI
mean_ci_t <- function(x, conf.level = 0.95) {
  x <- x[is.finite(x)]
  n <- length(x)
  if (n == 0) return(list(mean = NA_real_, lower = NA_real_, upper = NA_real_, n = 0))
  m <- mean(x)
  s <- sd(x)
  if (n < 2 || is.na(s) || s == 0) {
    return(list(mean = m, lower = NA_real_, upper = NA_real_, n = n))
  }
  alpha <- 1 - conf.level
  tcrit <- qt(1 - alpha/2, df = n - 1)
  se <- s / sqrt(n)
  list(mean = m, lower = m - tcrit * se, upper = m + tcrit * se, n = n)
}

# Resolve respiratory variable
resp_var <- if ("respiratory_signs" %in% names(df3)) {
  "respiratory_signs"
} else if ("respiratory_symptoms" %in% names(df3)) {
  "respiratory_symptoms"
} else {
  stop("Neither 'respiratory_signs' nor 'respiratory_symptoms' found in df3.")
}

# Ensure mean_ct exists
ct_var <- if ("mean_ct" %in% names(df3)) "mean_ct" else stop("'mean_ct' not found in df3.")

# Build analysis frame
resp_flag <- coerce_flag_logical(df3[[resp_var]])
ct <- suppressWarnings(as.numeric(df3[[ct_var]]))
group_label <- ifelse(resp_flag, "Respiratory signs observed (TRUE)",
                      ifelse(resp_flag == FALSE, "Clinically inapparent (FALSE)", NA))

# Per-group summaries (FALSE, TRUE, Overall)
summarize_group <- function(mask, label) {
  grp_size <- sum(mask, na.rm = TRUE)
  x <- ct[mask]
  ci <- mean_ci_t(x, conf.level = 0.95)
  n_nonmissing <- ci$n
  na_pct <- if (grp_size > 0) round(100 * (grp_size - n_nonmissing) / grp_size, 1) else NA_real_
  data.frame(
    variable = ct_var,
    group = label,
    group_size = grp_size,
    N_nonmissing = n_nonmissing,
    na_pct = na_pct,
    mean = round(ci$mean, 3),
    CI_lower_mean = round(ci$lower, 3),
    CI_upper_mean = round(ci$upper, 3),
    CI_95_mean = ifelse(is.na(ci$lower) | is.na(ci$upper), "", paste0("[", round(ci$lower, 3), ", ", round(ci$upper, 3), "]")),
    median = NA_real_,
    sd = NA_real_,
    IQR = NA_real_,
    Test = "",
    p_value = "",
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

mask_false <- resp_flag == FALSE
mask_true  <- resp_flag == TRUE
mask_all   <- !is.na(resp_flag)  # overall uses herds with known respiratory status

row_false <- summarize_group(mask_false, "Clinically inapparent (FALSE)")
row_true  <- summarize_group(mask_true,  "Respiratory signs observed (TRUE)")
row_all   <- summarize_group(mask_all,   "All herds")

ct_base <- dplyr::bind_rows(row_false, row_true, row_all)

# Welch two-sample t-test (TRUE vs FALSE)
x_true  <- ct[mask_true]
x_false <- ct[mask_false]
x_true  <- x_true[is.finite(x_true)]
x_false <- x_false[is.finite(x_false)]

tt <- if (length(x_true) >= 2 && length(x_false) >= 2) {
  tryCatch(t.test(x_true, x_false, var.equal = FALSE), error = function(e) NULL)
} else NULL

mean_diff <- if (length(x_true) && length(x_false)) mean(x_true) - mean(x_false) else NA_real_
ci_diff   <- if (!is.null(tt)) tt$conf.int else c(NA_real_, NA_real_)
p_val     <- if (!is.null(tt)) tt$p.value else NA_real_

p_row <- data.frame(
  variable   = ct_var,
  group      = "Mean difference (TRUE − FALSE)",
  group_size = NA_integer_,
  N_nonmissing = NA_integer_,
  na_pct     = NA_real_,
  mean       = round(mean_diff, 3),
  CI_lower_mean = round(ci_diff[1], 3),
  CI_upper_mean = round(ci_diff[2], 3),
  CI_95_mean    = ifelse(any(is.na(ci_diff[1:2])), "", paste0("[", round(ci_diff[1], 3), ", ", round(ci_diff[2], 3), "]")),
  median     = NA_real_,
  sd         = NA_real_,
  IQR        = NA_real_,
  Test       = "Welch two-sample t-test (means)",
  p_value    = ifelse(is.na(p_val), "", formatC(p_val, digits = 3, format = "g")),
  stringsAsFactors = FALSE,
  check.names = FALSE
)

ct_out <- dplyr::bind_rows(ct_base, p_row)

# Save CSV
out_dir <- "Publication_Outputs"
if (!dir.exists(out_dir)) dir.create(out_dir, showWarnings = FALSE)
outfile <- file.path(out_dir, "table_meanCt_by_respiratory_signs_ttest.csv")
readr::write_csv(ct_out, outfile)
message("Saved mean Ct comparison (Welch t-test) to: ", outfile)