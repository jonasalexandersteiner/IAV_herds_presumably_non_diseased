# SIV Logistic Regression Analysis Script (VIP-based Selection, Best Practice)
# Author: jonasalexandersteiner
# Date: 2025-09-17

# --- 0. Load required libraries ---
library(dplyr)
library(pROC)
library(car)
library(ResourceSelection) # For calibration test (Hosmer-Lemeshow)
library(rms)               # For calibration curve/plot
library(ggplot2)           # For ROC plotting and saving
library(rmarkdown)

# --- 0a. Output directory (ensure exists) ---
output_dir <- "C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/SIV_Projekt/Data_processing/SIV_farms100/04_output"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# --- 1. Data Preparation: VIP-based Variable Selection -----------------------

# 1.1 Load PLS-DA results and cleaned/dummy-encoded data
plsda_results <- readRDS(file.path(output_dir, "SIV_PLS_Analysis_Results/plsda_analysis_results_subsets.rds"))
# df3_pls must already be loaded in the workspace

# 1.2 Helper: Extract Top N VIP Predictors for a Subset (kept for reference, not used below)
get_top_vip <- function(plsda_subset, n = 25) {
  plsda_subset$coef_df$Predictor[order(-plsda_subset$coef_df$VIP)][1:n]
}

# 1.3 OVERRIDE: Use curated Top-25 VIP predictors (Husbandry) as provided
top25_husbandry <- c(
  "contact_bird_in_stable_TRUE",
  "ai_airspace_with_other_agegroup_2",
  "ai_airspace_with_other_agegroup_1",
  "production_cycle_3",
  "cleaning_weaner_stable_FALSE",
  "number_suckling_piglets",
  "caretaker_entry_ppe_only_TRUE",
  "bird_nests_FALSE",
  "farrowing_airspace_with_other_agegroup_2",
  "cross_fostering_farrowing_stable_3",
  "weaners_airspace_with_other_agegroup_3",
  "fattening_pigs_airspace_with_other_agegroup_4",
  "production_cycle_2",
  "proximity_to_other_poultry_herd_FALSE",
  "farrowing_airspace_with_other_agegroup_3",
  "outside_area_contact_wild_boars_FALSE",
  "number_weaners",
  "separation_quarantine_area_2",
  "cleaning_shipment_area_TRUE",
  "cattle_closeby_FALSE",
  "outside_area_contact_poultry_TRUE",
  "herdsize",
  "caretaker_ppe_washing_interval_FALSE",
  "mode_stable_occupation_ai_centre_TRUE",
  "proximity_to_other_pig_herd_TRUE"
)

# 1.4 Assemble VIP table for transparency and Rmd compatibility (restricted to curated Top-25)
vip_husbandry <- plsda_results$Husbandry$coef_df$VIP
names(vip_husbandry) <- plsda_results$Husbandry$coef_df$Predictor

vip_table_husbandry <- data.frame(
  variable = names(vip_husbandry),
  VIP = as.numeric(vip_husbandry),
  stringsAsFactors = FALSE
) %>%
  dplyr::filter(variable %in% top25_husbandry) %>%
  dplyr::mutate(rank_order = match(variable, top25_husbandry)) %>%
  dplyr::arrange(rank_order) %>%
  dplyr::select(-rank_order)

# Sanity check: warn if any curated variables are missing from the PLS-DA VIP list
missing_curated <- setdiff(top25_husbandry, vip_table_husbandry$variable)
if (length(missing_curated)) {
  message("Warning: The following curated Top-25 variables were not found in the PLS-DA VIP table: ",
          paste(missing_curated, collapse = ", "))
}

# 1.5 Biological plausibility exclusion (Husbandry): interpretation mapping
# Labels: "ok" -> keep; "intensive farming" -> standardized as confounding; 
# "class imbalance leads to spurious findings" -> standardized; "no effect" -> standardized.

interpret_map_raw <- c(
  "contact_bird_in_stable_TRUE"                   = "ok",
  "ai_airspace_with_other_agegroup_2"             = "intensive farming",
  "ai_airspace_with_other_agegroup_1"             = "intensive farming",
  "production_cycle_3"                            = "class imbalance leads to spurious findings",
  "cleaning_weaner_stable_FALSE"                  = "intensive farming",
  "number_suckling_piglets"                       = "no effect",
  "caretaker_entry_ppe_only_TRUE"                 = "intensive farming",
  "bird_nests_FALSE"                              = "intensive farming",
  "farrowing_airspace_with_other_agegroup_2"      = "intensive farming",
  "cross_fostering_farrowing_stable_3"            = "ok",
  "weaners_airspace_with_other_agegroup_3"        = "ok",
  "fattening_pigs_airspace_with_other_agegroup_4" = "intensive farming",
  "production_cycle_2"                            = "class imbalance leads to spurious findings",
  "proximity_to_other_poultry_herd_FALSE"         = "intensive farming",
  "farrowing_airspace_with_other_agegroup_3"      = "intensive farming",
  "outside_area_contact_wild_boars_FALSE"         = "intensive farming",
  "number_weaners"                                = "no effect",
  "separation_quarantine_area_2"                  = "intensive farming",
  "cleaning_shipment_area_TRUE"                   = "intensive farming",
  "cattle_closeby_FALSE"                          = "intensive farming",
  "outside_area_contact_poultry_TRUE"             = "intensive farming",
  "herdsize"                                      = "no effect",
  "caretaker_ppe_washing_interval_FALSE"          = "intensive farming",
  "mode_stable_occupation_ai_centre_TRUE"         = "intensive farming",
  "proximity_to_other_pig_herd_TRUE"              = "ok"
)

# Standardize reason wording (only for non-"ok" entries)
standardize_reason <- function(lbl) {
  if (is.na(lbl) || lbl == "ok") return("")
  switch(lbl,
         "intensive farming" = "Confounding: proxy for intensive farming",
         "class imbalance leads to spurious findings" = "Class imbalance: likely spurious finding",
         "no other age group" = "Exposure contrast: 'no other age group' (exclude)",
         "no effect" = "No effect in logistic regression (exclude)",
         lbl)
}

# Build exclusion table by joining VIP list with interpretation mapping
interp_df <- data.frame(
  variable = names(interpret_map_raw),
  interpretation = unname(interpret_map_raw),
  stringsAsFactors = FALSE
)

vip_table_husbandry <- vip_table_husbandry %>%
  dplyr::left_join(interp_df, by = "variable") %>%
  dplyr::mutate(
    interpretation = ifelse(is.na(interpretation), "ok", interpretation),
    exclusion_due_to_biological_plausibility = vapply(interpretation, standardize_reason, character(1))
  )

# 1.6 Select final 4 predictors: highest VIP among those without exclusion reason
select_final4 <- function(vip_table) {
  vip_table %>%
    dplyr::arrange(dplyr::desc(VIP)) %>%
    dplyr::filter(is.na(exclusion_due_to_biological_plausibility) |
                    exclusion_due_to_biological_plausibility == "" |
                    exclusion_due_to_biological_plausibility == "-") %>%
    dplyr::slice_head(n = 4) %>%
    dplyr::pull(variable)
}
final4_husbandry <- select_final4(vip_table_husbandry)

# Optional: print exclusions for transparency
excluded_items <- vip_table_husbandry %>%
  dplyr::filter(!is.na(exclusion_due_to_biological_plausibility) &
                  exclusion_due_to_biological_plausibility != "")
if (nrow(excluded_items)) {
  cat("\nExclusions (Husbandry):\n")
  print(excluded_items[, c("variable", "VIP", "exclusion_due_to_biological_plausibility")])
}

# 1.7 Prepare modeling dataset (with IAV_positive)
husbandry_vars <- unique(c(final4_husbandry, "IAV_positive"))
stopifnot(all(husbandry_vars %in% names(df3_pls)))
df_logit_husbandry <- df3_pls[, husbandry_vars] %>% na.omit()

# 1.8 Ensure IAV_positive is consistently coded as binary factor
prep_siv_factor <- function(x) {
  x <- as.factor(x)
  factor(
    ifelse(x %in% c(1, "1", TRUE, "TRUE", "positive"), "positive",
           ifelse(x %in% c(0, "0", FALSE, "FALSE", "negative"), "negative", NA)),
    levels = c("negative", "positive")
  )
}
df_logit_husbandry$IAV_positive <- prep_siv_factor(df_logit_husbandry$IAV_positive)

# --- Helper for safe formula creation (handles non-syntactic names) ---
backtick_vars <- function(vars) paste0("`", vars, "`")

# --- 2. Fit Logistic Regression Model (VIP-selected predictors) --------------
formula_husbandry <- as.formula(
  paste("IAV_positive ~", paste(backtick_vars(setdiff(husbandry_vars, "IAV_positive")), collapse = " + "))
)
fit_husbandry <- glm(formula_husbandry, data = df_logit_husbandry, family = binomial)

# --- 3. Model Summaries, Diagnostics, and Calibration --------------------------------------

# 3.1 Husbandry model diagnostics
summary_fit_husbandry  <- summary(fit_husbandry)

# Compute ROC robustly (explicit levels, consistent direction)
roc_obj_husbandry <- pROC::roc(
  df_logit_husbandry$IAV_positive,
  fitted(fit_husbandry),
  levels = c("negative", "positive"),
  direction = "<"
)
auc_value_husbandry    <- pROC::auc(roc_obj_husbandry)

# Save ROC plot (SVG)
roc_plot_husbandry <- pROC::ggroc(roc_obj_husbandry, colour = "steelblue", size = 1.2) +
  ggplot2::geom_abline(slope = 1, intercept = 0, linetype = "dashed", colour = "grey60") +
  ggplot2::labs(title = "ROC - Husbandry Logistic Model", x = "1 - Specificity", y = "Sensitivity") +
  ggplot2::annotate("text", x = 0.6, y = 0.2, label = paste0("AUC = ", round(as.numeric(auc_value_husbandry), 3)),
                    hjust = 0, size = 4) +
  ggplot2::theme_minimal(base_size = 13)

roc_file <- file.path(output_dir, "logit_ROC_husbandry.svg")
ggplot2::ggsave(filename = roc_file, plot = roc_plot_husbandry, width = 6, height = 5, device = "svg")

# Odds ratios with CIs, pseudo-R2, VIF, Cook's distance
or_ci_husbandry        <- exp(cbind(OR = coef(fit_husbandry), confint(fit_husbandry)))
nullmod_husbandry      <- glm(IAV_positive ~ 1, data = df_logit_husbandry, family = binomial)
pseudo_r2_husbandry    <- 1 - as.numeric(logLik(fit_husbandry)) / as.numeric(logLik(nullmod_husbandry))
vif_vals_husbandry     <- vif(fit_husbandry)
cooksd_husbandry       <- cooks.distance(fit_husbandry)

# ROC object also saved via save() below
roc_obj_husbandry      <- roc_obj_husbandry

# 3.2 Calibration assessment
# Hosmer-Lemeshow test
calib_hl_husbandry <- tryCatch({
  ResourceSelection::hoslem.test(
    as.numeric(df_logit_husbandry$IAV_positive) - 1,  # y must be 0/1
    fitted(fit_husbandry),
    g = 10
  )
}, error = function(e) NULL)

# Calibration plot using rms::calibrate
calib_plot_husbandry <- NULL
calib_rms_husbandry <- NULL
try({
  dd <- rms::datadist(df_logit_husbandry)
  options(datadist = "dd")
  fit_rms <- rms::lrm(formula_husbandry, data = df_logit_husbandry, x = TRUE, y = TRUE)
  calib_rms_husbandry <- rms::calibrate(fit_rms, method = "boot", B = 200)
  calib_plot_husbandry <- function() { plot(calib_rms_husbandry, main = "Calibration Curve (Husbandry Model)") }
}, silent = TRUE)

# --- 4. Save Results for R Markdown ------------------------------------------
save(
  summary_fit_husbandry, or_ci_husbandry, pseudo_r2_husbandry, auc_value_husbandry, vif_vals_husbandry, cooksd_husbandry,
  roc_obj_husbandry,
  calib_hl_husbandry, calib_rms_husbandry, calib_plot_husbandry,
  df_logit_husbandry,
  vip_table_husbandry,
  file = file.path(output_dir, "SIV_logistic_regression_results.RData")
)

# Save session info for reproducibility
writeLines(capture.output(sessionInfo()), file.path(output_dir, "SIV_logistic_regression_sessionInfo.txt"))

# --- 5. Render R Markdown Report ---------------------------------------------
render(
  input = "C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/SIV_Projekt/Data_processing/SIV_farms100/03_code/SIV_logistic_regression_results.Rmd",
  output_file = "SIV_logistic_regression_results.html",
  output_dir  = output_dir
)

# --- End of Script ---