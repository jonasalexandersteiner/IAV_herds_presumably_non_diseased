# Minimal script: read two-column coords and plot small blue points over Switzerland.
# Robust base-R parsing (no readr), no de-overlap, no jitter.

# Packages
if (!require(ggplot2)) install.packages("ggplot2")
if (!require(sf)) install.packages("sf")
if (!require(rnaturalearth)) install.packages("rnaturalearth")

library(ggplot2)
library(sf)
library(rnaturalearth)

# Paths (use forward slashes)
csv_path <- "C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/SIV_Projekt/Data_processing/SIV_farms100/01_oridata/Map_data.csv"
out_dir  <- "C:/Users/js22i117/OneDrive - Universitaet Bern/Dokumente/SIV_Projekt/Data_processing/SIV_farms100/04_output"

# Read file lines and parse two numeric columns (handles tab/comma/semicolon/space delimiters)
lines <- readLines(csv_path, warn = FALSE)
if (length(lines) == 0) stop("File is empty: ", csv_path)

# Drop header if present (e.g., 'X.1\tX.2')
if (grepl("[A-Za-z]", lines[1])) {
  lines <- lines[-1]
}

# Split each line by common delimiters and take the first two fields
parts <- strsplit(lines, "[,;\\t ]+")
lat <- suppressWarnings(as.numeric(vapply(parts, function(x) if (length(x) >= 2) x[1] else NA_character_, FUN.VALUE = character(1))))
lon <- suppressWarnings(as.numeric(vapply(parts, function(x) if (length(x) >= 2) x[2] else NA_character_, FUN.VALUE = character(1))))

# Build points and drop missing
pts <- data.frame(lon = lon, lat = lat)
pts <- pts[!is.na(pts$lon) & !is.na(pts$lat), , drop = FALSE]

if (nrow(pts) == 0) stop("No valid numeric points parsed. Check file delimiters and numeric formatting.")

# Load Switzerland layers
switz   <- rnaturalearth::ne_countries(scale = "large", country = "Switzerland", returnclass = "sf")
cantons <- rnaturalearth::ne_states(country = "Switzerland", returnclass = "sf")

# Plot: small blue points, no cropping to ensure visibility
p <- ggplot() +
  geom_sf(data = switz,   fill = "white", color = "black", linewidth = 1) +
  geom_sf(data = cantons, fill = NA,     color = "gray40", linewidth = 0.5) +
  geom_point(data = pts, aes(x = lon, y = lat), color = "#00008B", alpha = 0.9, size = 1.2) +
  theme_void()

# Save SVG
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
ggsave(filename = file.path(out_dir, "switzerland_points.svg"),
       plot = p, device = "svg", width = 8, height = 6, units = "in")

# Save PDF
ggsave(filename = file.path(out_dir, "switzerland_points.png"),
       plot = p, device = "png", width = 8, height = 6, units = "in", dpi = 300)