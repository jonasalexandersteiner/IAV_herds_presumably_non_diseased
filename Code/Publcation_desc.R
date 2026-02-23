# Publication descriptive outputs (compact) + optional inferential exports
# This script creates two descriptive tables with 95% CIs on the proportion IAV-positive
# and appends a single overall between-group p-value (Chi-squared or Fisher's exact):
#  - Age_weeks_factor x IAV_positive
#  - Production_type_factor x IAV_positive

# ===========================
# Libraries
# ===========================
library(dplyr)
library(tidyr)
library(readr)
library(tibble)

# ===========================
# Helpers
# ===========================

# Normalize IAV_positive to factor with levels negative/positive
normalize_iav_factor <- function(x) {
  x <- as.factor(x)
  factor(
    ifelse(x %in% c(1, "1", TRUE, "TRUE", "positive"), "positive",
           ifelse(x %in% c(0, "0", FALSE, "FALSE", "negative"), "negative", NA)),
    levels = c("negative", "positive")
  )
}

# Find a variable by case/alias in a data frame
find_var <- function(df, candidates) {
  for (nm in candidates) if (nm %in% names(df)) return(nm)
  low <- tolower(names(df))
  for (nm in tolower(candidates)) {
    idx <- which(low == nm)
    if (length(idx) == 1) return(names(df)[idx])
  }
  stop("None of the candidate variable names found: ", paste(candidates, collapse = ", "))
}

# Choose Chi-squared vs Fisher and return test label + p-value
chisq_or_fisher_from_table <- function(tbl) {
  if (!is.matrix(tbl) && !is.table(tbl)) return(list(test = "Not analyzable", p = NA_real_))
  if (any(dim(tbl) < 2) || sum(tbl) == 0) return(list(test = "Not analyzable", p = NA_real_))
  expected <- outer(rowSums(tbl), colSums(tbl)) / sum(tbl)
  if (any(expected < 5, na.rm = TRUE)) {
    p <- tryCatch(fisher.test(tbl)$p.value, error = function(e) NA_real_)
    list(test = "Fisher's exact", p = p)
  } else {
    p <- tryCatch(chisq.test(tbl, correct = FALSE)$p.value, error = function(e) NA_real_)
    list(test = "Chi-squared", p = p)
  }
}

# Build crosstab with counts, row %, and 95% CI (Wilson) for the proportion of target_level (default: "positive")
# Appends a final row with an overall between-group p-value across all levels of row_var
make_crosstab <- function(df, row_var, col_var = "IAV_positive", file_out,
                          target_level = "positive", conf.level = 0.95) {
  stopifnot(row_var %in% names(df), col_var %in% names(df))
  d <- df[, c(row_var, col_var), drop = FALSE]
  d <- d[!is.na(d[[row_var]]) & !is.na(d[[col_var]]), , drop = FALSE]
  
  # Ensure factors and drop unused row levels
  d[[row_var]] <- droplevels(as.factor(as.character(d[[row_var]])))
  d[[col_var]] <- as.factor(as.character(d[[col_var]]))
  
  # Counts by row_var x col_var
  tab <- table(d[[row_var]], d[[col_var]], dnn = c(row_var, col_var))
  tab <- as.matrix(tab)
  
  # Row percentages (for each outcome level) in percent
  row_pct <- round(suppressWarnings(prop.table(tab, 1) * 100), 1)
  
  # Counts to data frame
  out_counts <- as.data.frame.matrix(tab)
  out_counts <- tibble::rownames_to_column(out_counts, var = row_var)
  
  # Row % to data frame
  out_pct <- as.data.frame.matrix(row_pct)
  out_pct <- tibble::rownames_to_column(out_pct, var = row_var)
  
  # Merge counts and row %
  merged <- dplyr::left_join(out_counts, out_pct, by = row_var, suffix = c("_count", "_row_pct"))
  
  # Outcome base levels present in counts (e.g., "negative","positive")
  base_levels <- setdiff(names(out_counts), row_var)
  count_cols  <- paste0(base_levels, "_count")
  pct_cols    <- paste0(base_levels, "_row_pct")
  
  # Ensure column for the target level count exists even if absent in data
  pos_col <- paste0(target_level, "_count")
  if (!pos_col %in% names(merged)) {
    merged[[pos_col]] <- 0L
    count_cols <- union(count_cols, pos_col)
  }
  
  # Row totals from counts
  if (!all(count_cols %in% names(merged))) {
    stop("Expected count columns not found after join: ",
         paste(setdiff(count_cols, names(merged)), collapse = ", "))
  }
  merged$Row_Total <- rowSums(merged[, count_cols, drop = FALSE])
  
  # Wilson CI that always returns a 2-col matrix for vector inputs
  z <- qnorm(1 - (1 - conf.level) / 2)
  wilson_ci <- function(k, n) {
    L <- length(n)
    lo <- rep(NA_real_, L)
    hi <- rep(NA_real_, L)
    ok <- !is.na(n) & n > 0
    if (any(ok)) {
      p <- k[ok] / n[ok]
      denom  <- 1 + z^2 / n[ok]
      center <- p + z^2 / (2 * n[ok])
      adj    <- z * sqrt(p * (1 - p) / n[ok] + z^2 / (4 * n[ok]^2))
      lo[ok] <- pmax(0, (center - adj) / denom)
      hi[ok] <- pmin(1, (center + adj) / denom)
    }
    cbind(lo, hi)
  }
  
  # Proportion and CI for target_level per row
  k_vec <- merged[[pos_col]]
  n_vec <- merged$Row_Total
  ci_mat <- wilson_ci(k_vec, n_vec)  # matrix [nrow(merged) x 2]
  prop_vec <- ifelse(n_vec > 0, k_vec / n_vec, NA_real_)
  merged$Positive_Count       <- k_vec
  merged$Proportion_percent   <- round(100 * prop_vec, 1)
  merged$CI_lower_percent     <- round(100 * ci_mat[, 1], 1)
  merged$CI_upper_percent     <- round(100 * ci_mat[, 2], 1)
  
  # Totals row (counts only)
  col_totals <- colSums(as.data.frame.matrix(tab))
  totals_counts <- as.data.frame(t(col_totals))
  totals_counts[[row_var]] <- "Total"
  totals_counts <- totals_counts[, c(row_var, base_levels), drop = FALSE]
  names(totals_counts)[names(totals_counts) != row_var] <- paste0(names(totals_counts)[names(totals_counts) != row_var], "_count")
  if (!pos_col %in% names(totals_counts)) totals_counts[[pos_col]] <- 0L
  totals_counts$Row_Total <- sum(col_totals)
  
  # Add placeholders for row_pct columns to align
  for (pc in pct_cols) if (!pc %in% names(totals_counts)) totals_counts[[pc]] <- NA_real_
  
  # Totals proportion and CI
  k_tot <- totals_counts[[pos_col]][1]
  n_tot <- totals_counts$Row_Total[1]
  ci_tot <- wilson_ci(k_tot, n_tot)
  p_tot  <- ifelse(n_tot > 0, k_tot / n_tot, NA_real_)
  totals_counts$Positive_Count     <- k_tot
  totals_counts$Proportion_percent <- round(100 * p_tot, 1)
  totals_counts$CI_lower_percent   <- round(100 * ci_tot[, 1], 1)
  totals_counts$CI_upper_percent   <- round(100 * ci_tot[, 2], 1)
  
  # Align and bind totals
  for (nm in setdiff(names(merged), names(totals_counts))) totals_counts[[nm]] <- NA
  totals_counts <- totals_counts[, names(merged), drop = FALSE]
  final <- dplyr::bind_rows(merged, totals_counts)
  
  # Overall between-group p-value across all levels of row_var
  test_tbl <- table(d[[row_var]], d[[col_var]])
  test_res <- tryCatch(chisq_or_fisher_from_table(test_tbl),
                       error = function(e) list(test = "Not analyzable", p = NA_real_))
  
  # Ensure Test / p_value columns exist and are character
  if (!"Test" %in% names(final))   final$Test   <- ""
  final$Test <- as.character(final$Test)
  if (!"p_value" %in% names(final)) final$p_value <- ""
  final$p_value <- as.character(final$p_value)
  
  # Append final p-value row (overall difference across groups)
  p_row <- as.list(stats::setNames(rep(NA, length(names(final))), names(final)))
  p_row[[row_var]] <- "P-value (overall difference across groups)"
  p_row[["Test"]]  <- if (is.null(test_res$test) || is.na(test_res$test)) "" else as.character(test_res$test)
  p_row[["p_value"]] <- if (is.na(test_res$p)) "" else formatC(test_res$p, digits = 3, format = "g")
  final <- dplyr::bind_rows(final, as.data.frame(p_row, check.names = FALSE, stringsAsFactors = FALSE))
  
  readr::write_csv(final, file_out)
  invisible(final)
}

# ===========================
# 1) Build the two requested tables (WITH 95% CIs + overall p-values)
# ===========================
stopifnot(exists("df3"))

# Normalize IAV_positive
df3 <- df3 %>% mutate(IAV_positive = normalize_iav_factor(IAV_positive))

# Resolve variable names (case-insensitive)
age_var <- find_var(df3, c("Age_weeks_factor", "age_weeks_factor"))
prod_var <- find_var(df3, c("Production_type_factor", "production_type_factor"))

# Output directory
out_dir <- "Publication_Outputs"
dir.create(out_dir, showWarnings = FALSE)

# Table 1: Age_weeks_factor x IAV_positive WITH CIs and overall p-value
age_tab_file <- file.path(out_dir, "table_AgeWeeks_by_IAV.csv")
age_out <- make_crosstab(df3, row_var = age_var, col_var = "IAV_positive",
                         file_out = age_tab_file, target_level = "positive", conf.level = 0.95)

# Table 2: Production_type_factor x IAV_positive WITH CIs and overall p-value
prod_tab_file <- file.path(out_dir, "table_ProductionType_by_IAV.csv")
prod_out <- make_crosstab(df3, row_var = prod_var, col_var = "IAV_positive",
                          file_out = prod_tab_file, target_level = "positive", conf.level = 0.95)

# Also print the overall p-values to console
age_tbl <- table(df3[[age_var]], df3$IAV_positive)
age_test <- chisq_or_fisher_from_table(age_tbl)
cat(sprintf("Age_weeks_factor vs IAV_positive: %s p = %s\n",
            age_test$test, ifelse(is.na(age_test$p), "NA", formatC(age_test$p, digits = 3, format = "g"))))

prod_tbl <- table(df3[[prod_var]], df3$IAV_positive)
prod_test <- chisq_or_fisher_from_table(prod_tbl)
cat(sprintf("Production_type_factor vs IAV_positive: %s p = %s\n",
            prod_test$test, ifelse(is.na(prod_test$p), "NA", formatC(prod_test$p, digits = 3, format = "g"))))

message("Saved crosstabs (with 95% CIs and overall p-values) to:\n  ",
        age_tab_file, "\n  ", prod_tab_file)


# ===========================
# 2) Inferential tables export (if results_* objects exist)
# ===========================
output_top25_inferential <- function(results_assoc, results_corr, filename, top_n = 50) {
  assoc_valid <- results_assoc %>% filter(!is.na(pval))
  top_vars <- assoc_valid %>% arrange(pval) %>% slice_head(n = top_n) %>% pull(variable)
  assoc_sub <- results_assoc %>%
    filter(variable %in% top_vars) %>%
    transmute(
      Variable = variable,
      N = formatC(N, format = "f", digits = 2),
      `NA%` = formatC(pct_NA, format = "f", digits = 2),
      Test = test,
      `p-value (association)` = ifelse(is.na(pval), "", formatC(pval, format = "f", digits = 2)),
      `FDR p-value (association)` = ifelse(is.na(pval_fdr), "", formatC(pval_fdr, format = "f", digits = 2))
    )
  cor_sub <- results_corr %>%
    filter(variable %in% top_vars) %>%
    transmute(
      Variable = variable,
      Correlation = ifelse(is.na(coef), "", formatC(coef, format = "f", digits = 2)),
      `Test (correlation)` = test
    )
  merged <- dplyr::left_join(assoc_sub, cor_sub, by = "Variable") %>%
    arrange(`p-value (association)`)
  write.csv(merged, filename, row.names = FALSE, na = "")
}

if (exists("results_husbandry") && exists("corr_husbandry")) {
  output_top25_inferential(results_husbandry,   corr_husbandry,   file.path(out_dir, "publication_table_husbandry.csv"))
}
if (exists("results_animals") && exists("corr_animals")) {
  output_top25_inferential(results_animals,     corr_animals,     file.path(out_dir, "publication_table_animal_health.csv"))
}
if (exists("results_environment") && exists("corr_environment")) {
  output_top25_inferential(results_environment, corr_environment, file.path(out_dir, "publication_table_environment.csv"))
}
if (exists("results_human") && exists("corr_human")) {
  output_top25_inferential(results_human,       corr_human,       file.path(out_dir, "publication_table_human.csv"))
}

# ===========================
# 3) PLS-DA publication outputs (unchanged logic)
# ===========================
rds_path <- "C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/SIV_Projekt/Data_processing/SIV_farms100/04_output/SIV_PLS_Analysis_Results/plsda_analysis_results_subsets.rds"
subset_results <- tryCatch(readRDS(rds_path), error = function(e) NULL)

calc_metrics <- function(truth, pred_class, probs) {
  truth <- factor(truth, levels = c(FALSE, TRUE))
  pred_class <- factor(pred_class, levels = c(FALSE, TRUE))
  conf_mat <- table(True = truth, Predicted = pred_class)
  sens <- if ("TRUE" %in% rownames(conf_mat) && "TRUE" %in% colnames(conf_mat) && sum(conf_mat["TRUE", ]) > 0)
    conf_mat["TRUE", "TRUE"] / sum(conf_mat["TRUE", ]) else NA
  spec <- if ("FALSE" %in% rownames(conf_mat) && "FALSE" %in% colnames(conf_mat) && sum(conf_mat["FALSE", ]) > 0)
    conf_mat["FALSE", "FALSE"] / sum(conf_mat["FALSE", ]) else NA
  balanced_acc <- mean(c(sens, spec), na.rm = TRUE)
  auc_val <- tryCatch({
    roc_obj <- roc(truth, probs, levels = c(FALSE, TRUE), direction = "<")
    as.numeric(pROC::auc(roc_obj))
  }, error = function(e) NA)
  tibble(Metric = c("Sensitivity", "Specificity", "Balanced Accuracy", "AUC"),
         Value = round(c(sens, spec, balanced_acc, auc_val), 3))
}

get_merged_table <- function(res, subset_name) {
  vip_table <- as.data.frame(res$coef_df)
  if (!"Predictor" %in% colnames(vip_table)) colnames(vip_table)[1] <- "Predictor"
  loadings <- res$loadings_df[, 1, drop = TRUE]
  predictors <- vip_table$Predictor
  if (is.null(names(loadings)) || any(names(loadings) == "")) names(loadings) <- rownames(res$loadings_df)
  loadings_vec <- loadings[predictors]
  if (subset_name == "Husbandry") {
    assign_type <- ifelse(loadings_vec < 0, "Risk", "Protective")
  } else if (subset_name == "Animals") {
    assign_type <- ifelse(loadings_vec > 0, "Risk", "Protective")
  } else {
    assign_type <- rep(NA, length(loadings_vec))
  }
  risk_table <- tibble(Predictor = predictors,
                       Loading = as.numeric(loadings_vec),
                       Risk_Protective = assign_type)
  merged <- dplyr::left_join(vip_table, risk_table, by = "Predictor") %>%
    arrange(desc(VIP))
  merged
}

make_score_plot <- function(scores_df, IAV_positive) {
  if (!"IAV_positive" %in% colnames(scores_df)) {
    scores_df$IAV_positive <- factor(IAV_positive, levels = c(FALSE, TRUE), labels = c("negative", "positive"))
  }
  scores_df$IAV_positive <- factor(scores_df$IAV_positive, levels = c("negative", "positive"))
  ggplot(scores_df, aes(x = `Comp 1`, y = `Comp 2`, color = IAV_positive)) +
    geom_point(size = 3, alpha = 0.7) +
    stat_ellipse(aes(group = IAV_positive), linetype = 2, size = 1) +
    labs(x = "Component 1", y = "Component 2", color = "IAV Status") +
    theme_minimal(base_size = 14) +
    theme(plot.title = element_blank(), legend.position = "right", axis.title = element_text(size = 12))
}

make_roc_plot <- function(truth, probs) {
  roc_obj <- tryCatch(roc(truth, probs, levels = c(FALSE, TRUE), direction = "<"), error = function(e) NULL)
  auc_val <- if (!is.null(roc_obj)) round(as.numeric(pROC::auc(roc_obj)), 3) else NA
  roc_df <- if (!is.null(roc_obj)) {
    tibble(Specificity = 1 - roc_obj$specificities, Sensitivity = roc_obj$sensitivities)
  } else {
    tibble(Specificity = c(0, 1), Sensitivity = c(0, 1))
  }
  ggplot(roc_df, aes(x = Specificity, y = Sensitivity)) +
    geom_line(size = 1.2) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey") +
    labs(x = "1 - Specificity", y = "Sensitivity") +
    theme_minimal(base_size = 14) +
    theme(plot.title = element_blank(), legend.position = "none", axis.title = element_text(size = 12)) +
    annotate("text", x = 0.65, y = 0.15, label = paste0("AUC = ", ifelse(is.na(auc_val), "NA", auc_val)), hjust = 0, size = 4)
}

if (!is.null(subset_results)) {
  all_merged_tables <- tibble()
  all_conf_mats <- tibble()
  all_metrics <- tibble()
  all_panels <- list()
  
  for (subset_name in c("Husbandry", "Animals")) {
    res <- subset_results[[subset_name]]
    label <- ifelse(subset_name == "Husbandry", "Husbandry", "Animal Health")
    
    merged_table <- get_merged_table(res, subset_name)
    merged_table_top <- head(merged_table, 50)
    merged_table_top$Subset <- label
    all_merged_tables <- dplyr::bind_rows(all_merged_tables, merged_table_top)
    
    ncomp <- res$ncomp_optimal
    pred <- predict(res$plsda_model, res$X, dist = "max.dist", comp = ncomp)
    pred_class <- pred$class$max.dist[, ncomp]
    probs <- pred$predict[, "TRUE", ncomp]
    truth <- res$IAV_positive
    
    conf_mat <- table(True = truth, Predicted = pred_class)
    conf_mat_df <- as.data.frame.matrix(conf_mat)
    conf_mat_df$Subset <- label
    all_conf_mats <- dplyr::bind_rows(all_conf_mats, conf_mat_df)
    
    metrics <- calc_metrics(truth, pred_class, probs)
    metrics$Subset <- label
    all_metrics <- dplyr::bind_rows(all_metrics, metrics)
    
    score_plot <- make_score_plot(res$scores_df, res$IAV_positive)
    roc_plot <- make_roc_plot(truth, probs)
    panel <- arrangeGrob(score_plot, roc_plot, ncol = 2)
    all_panels[[subset_name]] <- panel
    
    cat("\n", label, "Top VIP predictors (sample):\n")
    print(utils::head(merged_table_top, 10))
    cat("\nPerformance metrics:\n")
    print(metrics)
  }
  
  write.csv(all_merged_tables, file.path(out_dir, "Top20_MergedTable_Combined.csv"), row.names = FALSE)
  write.csv(all_conf_mats, file.path(out_dir, "ConfusionMatrix_Combined.csv"), row.names = FALSE)
  write.csv(all_metrics, file.path(out_dir, "PerformanceMetrics_Combined.csv"), row.names = FALSE)
  
  panel_A <- arrangeGrob(
    textGrob("A", gp = gpar(fontsize = 28, fontface = "bold"), x = 0, just = "left"),
    all_panels[["Husbandry"]],
    ncol = 2,
    widths = unit.c(unit(0.05, "npc"), unit(0.95, "npc"))
  )
  panel_B <- arrangeGrob(
    textGrob("B", gp = gpar(fontsize = 28, fontface = "bold"), x = 0, just = "left"),
    all_panels[["Animals"]],
    ncol = 2,
    widths = unit.c(unit(0.05, "npc"), unit(0.95, "npc"))
  )
  final_combined <- arrangeGrob(panel_A, panel_B, ncol = 1, heights = c(1, 1))
  svg_file <- file.path(out_dir, "Score_ROC_Combined.svg")
  ggsave(svg_file, final_combined, width = 14, height = 12, device = "svg")
  
  cat("\nPLS-DA outputs exported to: ", out_dir, "\n")
} else {
  message("PLS-DA results not found or failed to load: ", rds_path, " — skipping PLS-DA outputs.")
}

# ===========================
# 4) Logistic regression exports (now also save ROC plot if present)
# ===========================
required_objs <- c("fit_husbandry", "roc_obj_husbandry", "df_logit_husbandry")
missing <- required_objs[!sapply(required_objs, function(obj) exists(obj, inherits = TRUE))]
if (length(missing) > 0) {
  message("Skipping logistic export; missing required objects: ", paste(missing, collapse = ", "))
} else {
  # Save a logistic ROC plot to the publication folder as well (title removed)
  try({
    auc_val <- pROC::auc(roc_obj_husbandry)
    roc_pub <- pROC::ggroc(roc_obj_husbandry, colour = "steelblue", size = 1.2) +
      ggplot2::geom_abline(slope = 1, intercept = 0, linetype = "dashed", colour = "grey60") +
      ggplot2::labs(title = NULL, x = "1 - Specificity", y = "Sensitivity") +
      ggplot2::annotate("text", x = 0.6, y = 0.2, label = paste0("AUC = ", round(as.numeric(auc_val), 3)),
                        hjust = 0, size = 4) +
      ggplot2::theme_minimal(base_size = 13)
    ggplot2::ggsave(file.path(out_dir, "logit_ROC_husbandry.svg"), roc_pub, width = 6, height = 5, device = "svg")
  }, silent = TRUE)
  
  # Also save a publication-ready coefficient table if the fit is available
  try({
    coef_sum <- summary(fit_husbandry)$coefficients
    ci_log   <- suppressMessages(confint(fit_husbandry))
    or       <- exp(coef(fit_husbandry))
    ci_low   <- exp(ci_log[, 1])
    ci_high  <- exp(ci_log[, 2])
    fmt_p <- function(p) ifelse(is.na(p), "", ifelse(p < 0.001, "<0.001", formatC(p, digits = 2, format = "f")))
    coef_table_pub <- data.frame(
      Predictor    = rownames(coef_sum),
      Estimate     = round(coef_sum[, "Estimate"], 3),
      SE           = round(coef_sum[, "Std. Error"], 3),
      `Odds ratio` = round(or[rownames(coef_sum)], 3),
      `95 % CI`    = paste0("[", round(ci_low[rownames(coef_sum)], 3), ", ", round(ci_high[rownames(coef_sum)], 3), "]"),
      `P>|z|`      = fmt_p(coef_sum[, "Pr(>|z|)"]),
      N            = nrow(df_logit_husbandry),
      row.names    = NULL,
      check.names  = FALSE
    )
    readr::write_csv(coef_table_pub, file.path(out_dir, "logit_coefficients_husbandry.csv"))
  }, silent = TRUE)
}

# Respiratory tables
# Creates two outputs:
# 1) IAV_positive x respiratory_signs crosstab with counts, row %, and 95% CI (Wilson) for proportion positive
#    NOW: adds a final row with p-value for the difference in IAV-positive proportion between respiratory_signs FALSE vs TRUE
#    (Fisher/Chi-squared; test reported)
# 2) Descriptive stats for merged sneezing/coughing (weaners + fatteners) and rectal temperatures,
#    split by respiratory_signs (TRUE/FALSE) AND including an Overall row,
#    NOW: adds a per-variable final row with p-value (Wilcoxon) for TRUE vs FALSE
#    and a combined "95% CI" column for the mean
# Outputs written to Publication_Outputs/*.csv

# ===========================
# Libraries
# ===========================
library(dplyr)
library(readr)
library(tibble)
library(forcats)

# ===========================
# Helpers
# ===========================

# Normalize IAV_positive to factor with levels negative/positive
normalize_iav_factor <- function(x) {
  x <- as.factor(x)
  factor(
    ifelse(x %in% c(1, "1", TRUE, "TRUE", "positive"), "positive",
           ifelse(x %in% c(0, "0", FALSE, "FALSE", "negative"), "negative", NA)),
    levels = c("negative", "positive")
  )
}

# Find a variable by case/alias in a data frame
find_var <- function(df, candidates) {
  for (nm in candidates) if (nm %in% names(df)) return(nm)
  low <- tolower(names(df))
  for (nm in tolower(candidates)) {
    idx <- which(low == nm)
    if (length(idx) == 1) return(names(df)[idx])
  }
  stop("None of the candidate variable names found: ", paste(candidates, collapse = ", "))
}

# Decide association test for a contingency table and return p-value
chisq_or_fisher_from_table <- function(tbl) {
  if (any(dim(tbl) < 2) || sum(tbl) == 0) {
    return(list(test = "Not analyzable", p = NA_real_))
  }
  expected <- outer(rowSums(tbl), colSums(tbl)) / sum(tbl)
  if (any(expected < 5, na.rm = TRUE)) {
    p <- tryCatch(fisher.test(tbl)$p.value, error = function(e) NA_real_)
    list(test = "Fisher's exact", p = p)
  } else {
    p <- tryCatch(chisq.test(tbl, correct = FALSE)$p.value, error = function(e) NA_real_)
    list(test = "Chi-squared", p = p)
  }
}

# Build crosstab with counts, row %, and 95% CI (Wilson) for the proportion of target_level (default: "positive")
# Adds a final row with p-value specifically comparing IAV-positive proportions between respiratory_signs FALSE vs TRUE
make_crosstab <- function(df, row_var, col_var = "IAV_positive", file_out,
                          target_level = "positive", conf.level = 0.95) {
  stopifnot(row_var %in% names(df), col_var %in% names(df))
  d <- df[, c(row_var, col_var), drop = FALSE]
  d <- d[!is.na(d[[row_var]]) & !is.na(d[[col_var]]), , drop = FALSE]
  
  # Ensure factors and drop unused row levels
  d[[row_var]] <- droplevels(as.factor(as.character(d[[row_var]])))
  d[[col_var]] <- as.factor(as.character(d[[col_var]]))
  
  # Counts by row_var x col_var
  tab <- as.matrix(table(d[[row_var]], d[[col_var]], dnn = c(row_var, col_var)))
  # Row percentages (for each outcome level) in percent
  row_pct <- round(suppressWarnings(prop.table(tab, 1) * 100), 1)
  
  # Counts to data frame
  out_counts <- as.data.frame.matrix(tab)
  out_counts <- tibble::rownames_to_column(out_counts, var = row_var)
  
  # Row % to data frame
  out_pct <- as.data.frame.matrix(row_pct)
  out_pct <- tibble::rownames_to_column(out_pct, var = row_var)
  
  # Merge counts and row %
  merged <- dplyr::left_join(out_counts, out_pct, by = row_var, suffix = c("_count", "_row_pct"))
  
  # Outcome base levels present in counts (e.g., "negative","positive")
  base_levels <- setdiff(names(out_counts), row_var)
  count_cols  <- paste0(base_levels, "_count")
  pct_cols    <- paste0(base_levels, "_row_pct")
  
  # Ensure column for the target level count exists even if absent in data
  pos_col <- paste0(target_level, "_count")
  if (!pos_col %in% names(merged)) {
    merged[[pos_col]] <- 0L
    count_cols <- union(count_cols, pos_col)
  }
  
  # Row totals from counts
  if (!all(count_cols %in% names(merged))) {
    stop("Expected count columns not found after join: ",
         paste(setdiff(count_cols, names(merged)), collapse = ", "))
  }
  merged$Row_Total <- rowSums(merged[, count_cols, drop = FALSE])
  
  # Wilson CI that always returns a 2-col matrix for vector inputs
  z <- qnorm(1 - (1 - conf.level) / 2)
  wilson_ci <- function(k, n) {
    L <- length(n)
    lo <- rep(NA_real_, L)
    hi <- rep(NA_real_, L)
    ok <- !is.na(n) & n > 0
    if (any(ok)) {
      p <- k[ok] / n[ok]
      denom  <- 1 + z^2 / n[ok]
      center <- p + z^2 / (2 * n[ok])
      adj    <- z * sqrt(p * (1 - p) / n[ok] + z^2 / (4 * n[ok]^2))
      lo[ok] <- pmax(0, (center - adj) / denom)
      hi[ok] <- pmin(1, (center + adj) / denom)
    }
    cbind(lo, hi)
  }
  
  # Proportion and CI for target_level per row
  k_vec <- merged[[pos_col]]
  n_vec <- merged$Row_Total
  ci_mat <- wilson_ci(k_vec, n_vec)  # matrix [nrow(merged) x 2]
  prop_vec <- ifelse(n_vec > 0, k_vec / n_vec, NA_real_)
  merged$Positive_Count       <- k_vec
  merged$Proportion_percent   <- round(100 * prop_vec, 1)
  merged$CI_lower_percent     <- round(100 * ci_mat[, 1], 1)
  merged$CI_upper_percent     <- round(100 * ci_mat[, 2], 1)
  merged$CI_95                <- ifelse(
    is.na(merged$CI_lower_percent) | is.na(merged$CI_upper_percent),
    "",
    paste0("[", merged$CI_lower_percent, ", ", merged$CI_upper_percent, "]")
  )
  
  # Totals row (counts only)
  col_totals <- colSums(as.data.frame.matrix(tab))
  totals_counts <- as.data.frame(t(col_totals))
  totals_counts[[row_var]] <- "Total"
  totals_counts <- totals_counts[, c(row_var, base_levels), drop = FALSE]
  names(totals_counts)[names(totals_counts) != row_var] <- paste0(names(totals_counts)[names(totals_counts) != row_var], "_count")
  if (!pos_col %in% names(totals_counts)) totals_counts[[pos_col]] <- 0L
  totals_counts$Row_Total <- sum(col_totals)
  
  # Add placeholders for row_pct columns to align
  for (pc in pct_cols) if (!pc %in% names(totals_counts)) totals_counts[[pc]] <- NA_real_
  
  # Totals proportion and CI
  k_tot <- totals_counts[[pos_col]][1]
  n_tot <- totals_counts$Row_Total[1]
  ci_tot <- wilson_ci(k_tot, n_tot)
  p_tot  <- ifelse(n_tot > 0, k_tot / n_tot, NA_real_)
  totals_counts$Positive_Count     <- k_tot
  totals_counts$Proportion_percent <- round(100 * p_tot, 1)
  totals_counts$CI_lower_percent   <- round(100 * ci_tot[, 1], 1)
  totals_counts$CI_upper_percent   <- round(100 * ci_tot[, 2], 1)
  totals_counts$CI_95              <- ifelse(
    is.na(totals_counts$CI_lower_percent) | is.na(totals_counts$CI_upper_percent),
    "",
    paste0("[", totals_counts$CI_lower_percent, ", ", totals_counts$CI_upper_percent, "]")
  )
  
  # Align and bind totals
  for (nm in setdiff(names(merged), names(totals_counts))) totals_counts[[nm]] <- NA
  totals_counts <- totals_counts[, names(merged), drop = FALSE]
  final <- dplyr::bind_rows(merged, totals_counts)
  
  # Association test p-value — specifically TRUE vs FALSE for respiratory_signs
  test_tbl <- table(d[[row_var]], d[[col_var]])
  rn <- rownames(test_tbl)
  # Prefer strict TRUE/FALSE rows if they both exist
  if (all(c("TRUE", "FALSE") %in% rn)) {
    test_tbl2 <- test_tbl[c("FALSE", "TRUE"), , drop = FALSE]
    label_row <- "P-value (positive proportion TRUE vs FALSE)"
  } else {
    test_tbl2 <- test_tbl
    label_row <- "P-value (association)"
  }
  test_res <- tryCatch(chisq_or_fisher_from_table(test_tbl2), error = function(e) list(test = "Not analyzable", p = NA_real_))
  
  # Ensure Test / p_value columns exist and are character to avoid bind_rows type issues
  if (!"Test" %in% names(final))   final$Test   <- ""
  final$Test <- as.character(final$Test)
  if (!"p_value" %in% names(final)) final$p_value <- ""
  final$p_value <- as.character(final$p_value)
  
  # Append final p-value row (build with character-safe fields)
  p_row <- as.list(stats::setNames(rep(NA, length(names(final))), names(final)))
  p_row[[row_var]] <- label_row
  p_row[["Test"]]  <- if (is.null(test_res$test) || is.na(test_res$test)) "" else as.character(test_res$test)
  p_row[["p_value"]] <- if (is.na(test_res$p)) "" else formatC(test_res$p, digits = 3, format = "g")
  final <- dplyr::bind_rows(final, as.data.frame(p_row, check.names = FALSE, stringsAsFactors = FALSE))
  
  readr::write_csv(final, file_out)
  invisible(final)
}

# Helper: 95% CI for mean using t-distribution (robust to small n)
mean_ci <- function(x, conf.level = 0.95) {
  x <- suppressWarnings(as.numeric(x))
  x <- x[!is.na(x)]
  n <- length(x)
  if (n <= 1) return(c(NA_real_, NA_real_, n))
  m  <- mean(x)
  s  <- stats::sd(x)
  se <- s / sqrt(n)
  alpha <- 1 - conf.level
  tcrit <- stats::qt(1 - alpha/2, df = n - 1)
  c(m - tcrit * se, m + tcrit * se, n)
}

# Summariser: per respiratory_signs group and Overall, with mean CI
# NOW: adds a combined CI_95_mean column and a per-variable p-value row (Wilcoxon TRUE vs FALSE)
summarize_continuous_by_group_with_overall <- function(df, vars, group_var, conf.level = 0.95) {
  out_list <- lapply(vars, function(v) {
    d <- df[, c(group_var, v), drop = FALSE]
    names(d) <- c("group", "value")
    d$group <- as.character(d$group)
    d$value <- suppressWarnings(as.numeric(d$value))
    
    # Per-group stats
    grp_sizes <- as.data.frame(table(d$group), stringsAsFactors = FALSE)
    names(grp_sizes) <- c("group", "group_size")
    stats_by_group <- d %>%
      dplyr::group_by(group) %>%
      dplyr::summarise(
        N_nonmissing = sum(!is.na(value)),
        mean   = ifelse(N_nonmissing > 0, round(mean(value, na.rm = TRUE), 3), NA_real_),
        median = ifelse(N_nonmissing > 0, round(median(value, na.rm = TRUE), 3), NA_real_),
        sd     = ifelse(N_nonmissing > 1, round(sd(value, na.rm = TRUE), 3), NA_real_),
        IQR    = ifelse(N_nonmissing > 0, round(IQR(value, na.rm = TRUE), 3), NA_real_),
        CI_vals = list(mean_ci(value, conf.level = conf.level)),
        .groups = "drop"
      ) %>%
      dplyr::mutate(
        CI_lower_mean = round(vapply(CI_vals, function(z) z[1], numeric(1)), 3),
        CI_upper_mean = round(vapply(CI_vals, function(z) z[2], numeric(1)), 3)
      ) %>%
      dplyr::select(-CI_vals) %>%
      dplyr::left_join(grp_sizes, by = "group") %>%
      dplyr::mutate(
        na_pct  = round(100 * (group_size - N_nonmissing) / dplyr::if_else(group_size == 0, 1, group_size), 1),
        variable = v,
        CI_95_mean = ifelse(
          is.na(CI_lower_mean) | is.na(CI_upper_mean),
          "",
          paste0("[", CI_lower_mean, ", ", CI_upper_mean, "]")
        )
      ) %>%
      dplyr::select(variable, group, group_size, N_nonmissing, na_pct, mean, CI_lower_mean, CI_upper_mean, CI_95_mean, median, sd, IQR)
    
    # Overall stats (all herds)
    overall_n_total <- nrow(d)
    overall_n_nonmissing <- sum(!is.na(d$value))
    overall_ci <- mean_ci(d$value, conf.level = conf.level)
    overall_row <- data.frame(
      variable      = v,
      group         = "Overall",
      group_size    = overall_n_total,
      N_nonmissing  = overall_n_nonmissing,
      na_pct        = round(100 * (overall_n_total - overall_n_nonmissing) / ifelse(overall_n_total == 0, 1, overall_n_total), 1),
      mean          = ifelse(overall_n_nonmissing > 0, round(mean(d$value, na.rm = TRUE), 3), NA_real_),
      CI_lower_mean = round(overall_ci[1], 3),
      CI_upper_mean = round(overall_ci[2], 3),
      CI_95_mean    = ifelse(any(is.na(overall_ci[1:2])), "", paste0("[", round(overall_ci[1], 3), ", ", round(overall_ci[2], 3), "]")),
      median        = ifelse(overall_n_nonmissing > 0, round(median(d$value, na.rm = TRUE), 3), NA_real_),
      sd            = ifelse(overall_n_nonmissing > 1, round(sd(d$value, na.rm = TRUE), 3), NA_real_),
      IQR           = ifelse(overall_n_nonmissing > 0, round(IQR(d$value, na.rm = TRUE), 3), NA_real_),
      stringsAsFactors = FALSE
    )
    
    # P-value (TRUE vs FALSE) using Wilcoxon if both groups have data
    x_true  <- d$value[d$group %in% c("TRUE", "true", "1")]
    x_false <- d$value[d$group %in% c("FALSE", "false", "0")]
    has_both <- sum(!is.na(x_true)) > 0 && sum(!is.na(x_false)) > 0
    p_val <- if (has_both) tryCatch(wilcox.test(x_true, x_false, exact = FALSE)$p.value, error = function(e) NA_real_) else NA_real_
    p_row <- data.frame(
      variable = v,
      group = "P-value (TRUE vs FALSE)",
      group_size = NA_integer_,
      N_nonmissing = NA_integer_,
      na_pct = NA_real_,
      mean = NA_real_,
      CI_lower_mean = NA_real_,
      CI_upper_mean = NA_real_,
      CI_95_mean = "",
      median = NA_real_,
      sd = NA_real_,
      IQR = NA_real_,
      Test = "Wilcoxon Mann–Whitney",
      p_value = ifelse(is.na(p_val), "", formatC(p_val, digits = 3, format = "g")),
      stringsAsFactors = FALSE
    )
    
    out <- dplyr::bind_rows(stats_by_group, overall_row)
    # Add empty Test / p_value columns to align and enforce character type
    if (!"Test" %in% names(out))   out$Test <- ""
    out$Test <- as.character(out$Test)
    if (!"p_value" %in% names(out)) out$p_value <- ""
    out$p_value <- as.character(out$p_value)
    
    dplyr::bind_rows(out, p_row)
  })
  dplyr::bind_rows(out_list)
}

# ===========================
# Main
# ===========================
stopifnot(exists("df3"))

# Normalize IAV_positive
df3 <- df3 %>% dplyr::mutate(IAV_positive = normalize_iav_factor(IAV_positive))

# Output directory
out_dir <- "Publication_Outputs"
dir.create(out_dir, showWarnings = FALSE)

# ---------- Table 1: IAV_positive x respiratory_signs (with CI and final p-value row) ----------
resp_var <- find_var(df3, c("respiratory_signs"))
# Coerce to factor; keep NA as explicit level if present (still excluded from tests/crosstab)
df3[[resp_var]] <- as.factor(df3[[resp_var]])
# Replace deprecated fct_explicit_na with fct_na_value_to_level
df3[[resp_var]] <- forcats::fct_na_value_to_level(df3[[resp_var]], level = "NA")

file_resp_iav <- file.path(out_dir, "table_IAV_by_respiratory_signs.csv")
ct_out <- make_crosstab(
  df3, row_var = resp_var, col_var = "IAV_positive",
  file_out = file_resp_iav, target_level = "positive", conf.level = 0.95
)

# ---------- Table 2: merged sneezing/coughing + rectal temps by respiratory_signs (WITH Overall, mean CI, and p-value row) ----------
# Find variable names
sneeze_weaners_var   <- find_var(df3, c("weaners_sneezing"))
sneeze_fatteners_var <- find_var(df3, c("fattening_pigs_sneezing"))
cough_weaners_var    <- find_var(df3, c("weaners_coughing"))
cough_fatteners_var  <- find_var(df3, c("fattening_pigs_coughing"))
rectal_avg_var       <- find_var(df3, c("rectal_temperature_avg"))
rectal_max_var       <- find_var(df3, c("rectal_temperature_max"))

# Create merged variables: prefer weaners value; if NA, use fatteners value
to_num <- function(x) suppressWarnings(as.numeric(x))
df3$sneezing_merged <- dplyr::coalesce(to_num(df3[[sneeze_weaners_var]]), to_num(df3[[sneeze_fatteners_var]]))
df3$coughing_merged <- dplyr::coalesce(to_num(df3[[cough_weaners_var]]),  to_num(df3[[cough_fatteners_var]]))

# Build and save table
file_resp_group_stats <- file.path(out_dir, "table_RespiratorySigns_Grouped_MergedWeaners_Fatteners_Rectal.csv")
tbl_resp_group <- summarize_continuous_by_group_with_overall(
  df3,
  vars = c("sneezing_merged", "coughing_merged", rectal_avg_var, rectal_max_var),
  group_var = resp_var,
  conf.level = 0.95
)
readr::write_csv(tbl_resp_group, file_resp_group_stats)

message("Saved:\n  ", file_resp_iav, "\n  ", file_resp_group_stats)
