# Set working directory
setwd("C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/IVA_Projekt/Data_processing/SIV_farms/04_output/SIV_PLS_Analysis_Results")

# Load required libraries
library(tidyverse)
library(rmarkdown)

# Helper function to calculate OR for binary predictors
get_odds_ratio <- function(x, y) {
  x <- as.numeric(x)
  result <- c(OR=NA, CI_low=NA, CI_high=NA, pval=NA)
  if (length(unique(na.omit(x))) == 2 && length(unique(na.omit(y))) == 2) {
    df <- data.frame(x=x, y=y)
    df <- df[complete.cases(df), ]
    if (nrow(df) < 5) return(result)
    fit <- tryCatch(glm(y ~ x, data=df, family=binomial), error=function(e) NULL)
    if (!is.null(fit)) {
      or <- exp(coef(fit)[2])
      ci <- tryCatch(exp(confint(fit)[2,]), error=function(e) c(NA,NA))
      pval <- summary(fit)$coefficients[2,4]
      result <- c(OR=or, CI_low=ci[1], CI_high=ci[2], pval=pval)
    }
  }
  result
}

# Load the original results
plsda_results <- readRDS("plsda_analysis_results_subsets.rds")

# For each subset, calculate odds ratios and add to results
subset_names <- names(plsda_results)
for (subset_name in subset_names) {
  subset_results <- plsda_results[[subset_name]]
  if (!is.null(subset_results)) {
    X_mat <- subset_results$X
    SIV_positive <- subset_results$SIV_positive
    predictors <- rownames(subset_results$loadings_df)
    or_results <- lapply(predictors, function(v) get_odds_ratio(X_mat[,v], SIV_positive))
    or_df <- tibble(
      Predictor = predictors,
      OR = sapply(or_results, function(x) x["OR"]),
      CI_low = sapply(or_results, function(x) x["CI_low"]),
      CI_high = sapply(or_results, function(x) x["CI_high"]),
      pval = sapply(or_results, function(x) x["pval"])
    )
    # Save to results object for Rmd rendering
    plsda_results[[subset_name]]$or_df <- or_df
  }
}

# Save the updated results
saveRDS(plsda_results, "plsda_analysis_results_subsets_with_ORs.rds")

# Render the Rmd, passing the enriched results as a parameter
rmarkdown::render(
  input = "ORs.Rmd",
  output_file = "ORs.html",
  output_dir = getwd(),
  params = list(plsda_results = plsda_results)
)

cat("Done! Results HTML written to ORs.html\n")