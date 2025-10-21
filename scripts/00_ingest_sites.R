# scripts/00_ingest_sites.R
# Ingest and standardize Sentinel Site coordinates from partner CSVs
# Output:
#   data/processed/SSN_All_Sites.gpkg (layer = "SSN_All_Sites", EPSG:4326)
#   outputs/tables/SSN_All_Sites.csv  (tabular copy without geometry)

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(janitor)
  library(sf)
})

# ------------------------------------------------------------------------------
# 0) Paths (project-relative)
# ------------------------------------------------------------------------------

in_dir        <- "data/SSN_site_coordinates"
out_gpkg_dir  <- "data/processed"
out_tbl_dir   <- "outputs/tables"

dir.create(out_gpkg_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_tbl_dir,  recursive = TRUE, showWarnings = FALSE)

cdfw_csv      <- file.path(in_dir, "CDFW_coords.csv")
ucnrs_csv     <- file.path(in_dir, "SSN_UCNRS_coords.csv")
tnc_csv       <- file.path(in_dir, "TNC_Pepperwood_coords.csv")

out_gpkg      <- file.path(out_gpkg_dir, "SSN_All_Sites.gpkg")
out_layer     <- "SSN_All_Sites"
out_csv       <- file.path(out_tbl_dir, "SSN_All_Sites.csv")

# ------------------------------------------------------------------------------
# 1) Helper function
# ------------------------------------------------------------------------------

standardize_sites <- function(path, org_value, site_col, lat_col, lon_col) {
  message("Reading: ", basename(path))
  
  df_raw <- read_csv(path, show_col_types = FALSE) |>
    janitor::clean_names()
  
  # Drop known empty columns (x4, x5) if present
  drop_cols <- intersect(names(df_raw), c("x4", "x5"))
  if (length(drop_cols) > 0) {
    df_raw <- select(df_raw, -all_of(drop_cols))
  }
  
  df <- df_raw |>
    rename(
      site_name = !!sym(site_col),
      lat = !!sym(lat_col),
      lon = !!sym(lon_col)
    ) |>
    mutate(
      org = org_value,
      lat = suppressWarnings(as.numeric(lat)),
      lon = suppressWarnings(as.numeric(lon))
    ) |>
    filter(!is.na(lat), !is.na(lon)) |>
    filter(between(lat, -90, 90), between(lon, -180, 180)) |>
    select(org, site_name, lon, lat, everything()) |>
    distinct(org, site_name, lon, lat, .keep_all = TRUE)
  
  df
}

# ------------------------------------------------------------------------------
# 2) Read and standardize each partner dataset
# ------------------------------------------------------------------------------

cdfw <- standardize_sites(
  path = cdfw_csv,
  org_value = "CDFW",
  site_col = "cdfw_sentinel_site_property_name",
  lat_col  = "latitute",       # note the typo in source
  lon_col  = "longitude"
)

ucnrs <- standardize_sites(
  path = ucnrs_csv,
  org_value = "UCNRS",
  site_col = "nrs_site_name",
  lat_col  = "lat",
  lon_col  = "lon"
)

tnc <- standardize_sites(
  path = tnc_csv,
  org_value = "TNC_Pepperwood",
  site_col = "tnc_sentinel_site",
  lat_col  = "latitute",       # same typo
  lon_col  = "longitude"
)

# ------------------------------------------------------------------------------
# 3) Combine and finalize
# ------------------------------------------------------------------------------

sites_combined <- bind_rows(cdfw, ucnrs, tnc) |>
  mutate(
    site_id = paste0("SSN_", org, "_", row_number())
  ) |>
  select(org, site_id, site_name, lon, lat, everything())

message("Row counts by org:")
print(sites_combined |> count(org))

# ------------------------------------------------------------------------------
# 4) Convert to sf (WGS84 / EPSG:4326)
# ------------------------------------------------------------------------------

sites_sf <- st_as_sf(sites_combined, coords = c("lon", "lat"), crs = 4326, remove = FALSE)

# ------------------------------------------------------------------------------
# 5) Write outputs
# ------------------------------------------------------------------------------

if (file.exists(out_gpkg)) {
  try(st_delete(out_gpkg, layer = out_layer), silent = TRUE)
}
st_write(sites_sf, out_gpkg, layer = out_layer, delete_layer = TRUE, quiet = TRUE)
readr::write_csv(st_drop_geometry(sites_sf), out_csv)

message("Wrote GeoPackage: ", normalizePath(out_gpkg))
message("Wrote CSV:        ", normalizePath(out_csv))