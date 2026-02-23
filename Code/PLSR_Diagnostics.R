# ===========================
# Diagnostics for PLS-DA Model Output (mixOmics::plsda)
# ===========================
# Prints and saves diagnostics to a report file.
# -----------------------------------------------------------
# Author: jonasalexandersteiner (2024)
# ===========================

diagnostics_file <- "plsda_diagnostics_report.txt"
sink(diagnostics_file, split = TRUE) # Save output to both file and console

cat("=== PLS-DA Model Output Diagnostics ===\n")

# 1. Loadings diagnostics
cat("\n--- Loadings ---\n")
loadings_X <- plsda_model$loadings$X
cat("Any NA in loadings? ", any(is.na(loadings_X)), "\n")
cat("Any all-zero loading columns? ", any(apply(loadings_X, 2, function(x) all(x == 0))), "\n")
cat("Any all-equal loading columns? ", any(apply(loadings_X, 2, function(x) length(unique(x)) == 1)), "\n")

# Highly correlated loading columns
if (ncol(loadings_X) > 1) {
  load_cor <- cor(loadings_X)
  diag(load_cor) <- 0
  high_cor_pairs <- which(abs(load_cor) > 0.99, arr.ind = TRUE)
  if (nrow(high_cor_pairs) > 0) {
    cat("Highly correlated loadings columns (|r| > 0.99):\n")
    print(high_cor_pairs)
  } else {
    cat("No highly correlated loadings columns (|r| > 0.99).\n")
  }
} else {
  cat("Only one component in loadings; correlation check skipped.\n")
}

# 2. VIP diagnostics
cat("\n--- VIP (Variable Importance in Projection) ---\n")
vip_scores <- tryCatch(mixOmics::vip(plsda_model), error=function(e) NULL)
if (!is.null(vip_scores)) {
  cat("Any NA in VIP? ", any(is.na(vip_scores)), "\n")
  cat("Any all-zero VIP columns? ", any(apply(vip_scores, 2, function(x) all(x == 0))), "\n")
  cat("Any all-equal VIP columns? ", any(apply(vip_scores, 2, function(x) length(unique(x)) == 1)), "\n")
} else {
  cat("Unable to extract VIP scores from model.\n")
}

# 3. Sample scores diagnostics
cat("\n--- Sample Scores ---\n")
scores_X <- plsda_model$variates$X
cat("Any NA in sample scores? ", any(is.na(scores_X)), "\n")
cat("Any all-zero sample score columns? ", any(apply(scores_X, 2, function(x) all(x == 0))), "\n")
cat("Any all-equal sample score columns? ", any(apply(scores_X, 2, function(x) length(unique(x)) == 1)), "\n")

# 4. Degenerate/confused predictions: Check confusion matrix and class balance (if predictions available)
cat("\n--- Class Predictions & Confusion Matrix ---\n")
if (exists("X") && !is.null(X)) {
  ncomp_used <- plsda_model$ncomp
  pred <- predict(plsda_model, X, dist = "max.dist", comp = ncomp_used)
  pred_class <- pred$class$max.dist[, ncomp_used]
  cat("Prediction class table:\n")
  print(table(pred_class))
  if (exists("SIV_positive")) {
    true_class <- SIV_positive
    cm <- table(True = true_class, Predicted = pred_class)
    cat("Confusion matrix:\n")
    print(cm)
    if (all(rowSums(cm) == 0) || all(colSums(cm) == 0)) {
      cat("Degenerate confusion matrix: only one class predicted or observed.\n")
    }
  }
} else {
  cat("X (matrix for prediction) not found, skipping confusion matrix check.\n")
}

# 5. AUC and Balanced Accuracy (if pROC installed and SIV_positive exists)
cat("\n--- AUC & Balanced Accuracy ---\n")
if (requireNamespace("pROC", quietly = TRUE) && exists("SIV_positive")) {
  pred <- predict(plsda_model, X, dist = "max.dist", comp = ncomp_used)
  class_names <- dimnames(pred$predict)[[2]]
  pos_class <- if ("positive" %in% class_names) "positive" else class_names[2]
  probs <- pred$predict[, pos_class, ncomp_used]
  roc_obj <- pROC::roc(SIV_positive, probs, levels=levels(SIV_positive), direction="<")
  auc_val <- as.numeric(pROC::auc(roc_obj))
  cat("AUC: ", auc_val, "\n")
  # Compute balanced accuracy
  cm <- table(True = SIV_positive, Predicted = pred_class)
  sens <- if ("positive" %in% rownames(cm) && "positive" %in% colnames(cm)) {
    cm["positive", "positive"] / sum(cm["positive", ])
  } else { NA }
  spec <- if ("negative" %in% rownames(cm) && "negative" %in% colnames(cm)) {
    cm["negative", "negative"] / sum(cm["negative", ])
  } else { NA }
  bal_acc <- mean(c(sens, spec), na.rm=TRUE)
  cat("Balanced accuracy: ", bal_acc, "\n")
} else {
  cat("pROC package or SIV_positive not available, skipping AUC/balanced accuracy.\n")
}

cat("\n=== End of Diagnostics ===\n")
sink() # Stop diverting output