# scripts/00_ingest_sites.R
# Ingest and standardize Sentinel Site coordinates from partner CSVs
# Outputs:
#   - data/processed/SSN_All_Sites.gpkg (layer = "SSN_All_Sites", EPSG:4326)
#   - outputs/tables/SSN_All_Sites.csv  (tabular copy without geometry)

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(janitor)
  library(sf)
  library(stringr)
})

# ------------------------------------------------------------------------------
# 0) Paths (project-relative)
# ------------------------------------------------------------------------------

in_dir        <- "data/SSN_site_coordinates"
out_gpkg_dir  <- "data/processed"
out_tbl_dir   <- "outputs/tables"
ref_dir       <- "data/reference"

dir.create(out_gpkg_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_tbl_dir,  recursive = TRUE, showWarnings = FALSE)

cdfw_csv  <- file.path(in_dir, "CDFW_coords.csv")
ucnrs_csv <- file.path(in_dir, "SSN_UCNRS_coords.csv")
tnc_csv   <- file.path(in_dir, "TNC_Pepperwood_coords.csv")

# Optional overrides file: columns = site_name, org
overrides_csv <- file.path(ref_dir, "org_overrides.csv")
has_overrides <- file.exists(overrides_csv)

out_gpkg  <- file.path(out_gpkg_dir, "SSN_All_Sites.gpkg")
out_layer <- "SSN_All_Sites"
out_csv   <- file.path(out_tbl_dir, "SSN_All_Sites.csv")

# ------------------------------------------------------------------------------
# 1) Helper function
# ------------------------------------------------------------------------------

standardize_sites <- function(path, org_value, site_col, lat_col, lon_col) {
  message("Reading: ", basename(path))
  df_raw <- readr::read_csv(path, show_col_types = FALSE) |>
    janitor::clean_names()
  
  # Drop known empty columns (x4, x5) if present
  drop_cols <- intersect(names(df_raw), c("x4", "x5"))
  if (length(drop_cols) > 0) {
    df_raw <- dplyr::select(df_raw, -dplyr::all_of(drop_cols))
  }
  
  df <- df_raw |>
    dplyr::rename(
      site_name = !!rlang::sym(site_col),
      lat       = !!rlang::sym(lat_col),
      lon       = !!rlang::sym(lon_col)
    ) |>
    dplyr::mutate(
      org = org_value,
      lat = suppressWarnings(as.numeric(lat)),
      lon = suppressWarnings(as.numeric(lon))
    ) |>
    dplyr::filter(!is.na(lat), !is.na(lon)) |>
    dplyr::filter(dplyr::between(lat, -90, 90), dplyr::between(lon, -180, 180)) |>
    dplyr::select(org, site_name, lon, lat, dplyr::everything()) |>
    dplyr::distinct(org, site_name, lon, lat, .keep_all = TRUE)
  
  df
}

# ------------------------------------------------------------------------------
# 2) Read and standardize each partner dataset
# ------------------------------------------------------------------------------

cdfw <- standardize_sites(
  path = cdfw_csv,
  org_value = "CDFW",
  site_col = "cdfw_sentinel_site_property_name",
  lat_col  = "latitute",   # typo in source
  lon_col  = "longitude"
)

ucnrs <- standardize_sites(
  path = ucnrs_csv,
  org_value = "UCNRS",
  site_col = "nrs_site_name",
  lat_col  = "lat",
  lon_col  = "lon"
)

# Read TNC/Pepperwood with org_value = NA; classify below
tnc_raw <- standardize_sites(
  path = tnc_csv,
  org_value = NA_character_,
  site_col = "tnc_sentinel_site",
  lat_col  = "latitute",   # same typo
  lon_col  = "longitude"
)

# Apply overrides if present; otherwise use name-based rule
if (has_overrides) {
  message("Applying org overrides from: ", overrides_csv)
  overrides <- readr::read_csv(overrides_csv, show_col_types = FALSE) |>
    janitor::clean_names() |>
    dplyr::select(site_name, org)
  tnc <- tnc_raw |>
    dplyr::left_join(overrides, by = "site_name") |>
    dplyr::mutate(
      org = dplyr::coalesce(org.y, org.x, "TNC")
    ) |>
    dplyr::select(-org.x, -org.y)
} else {
  message("No overrides file found. Classifying TNC vs Pepperwood by site_name pattern.")
  tnc <- tnc_raw |>
    dplyr::mutate(
      org = dplyr::case_when(
        str_detect(tolower(site_name %||% ""), "pepperwood") ~ "Pepperwood",
        TRUE                                                ~ "TNC"
      )
    )
}

# ------------------------------------------------------------------------------
# 3) Combine and finalize
# ------------------------------------------------------------------------------

sites_combined <- dplyr::bind_rows(cdfw, ucnrs, tnc) |>
  dplyr::mutate(
    org = as.character(org),
    site_id = paste0("SSN_", org, "_", dplyr::row_number())
  ) |>
  dplyr::select(org, site_id, site_name, lon, lat, dplyr::everything())

message("Row counts by org:")
print(sites_combined |> dplyr::count(org, sort = TRUE))

# ------------------------------------------------------------------------------
# 4) Convert to sf (WGS84 / EPSG:4326)
# ------------------------------------------------------------------------------

sites_sf <- sf::st_as_sf(sites_combined, coords = c("lon", "lat"), crs = 4326, remove = FALSE)

# ------------------------------------------------------------------------------
# 5) Write outputs
# ------------------------------------------------------------------------------

if (file.exists(out_gpkg)) {
  try(sf::st_delete(out_gpkg, layer = out_layer), silent = TRUE)
}
sf::st_write(sites_sf, out_gpkg, layer = out_layer, delete_layer = TRUE, quiet = TRUE)
readr::write_csv(sf::st_drop_geometry(sites_sf), out_csv)

message("Wrote GeoPackage: ", normalizePath(out_gpkg))
message("Wrote CSV:        ", normalizePath(out_csv))

# ------------------------------------------------------------------------------
# 6) Utilities
# ------------------------------------------------------------------------------

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b
