# Fetch daily NO2 statistics from the Sentinel Hub Statistical API in parallel for APAC on Windows

# Settings ---------------------------------------------------------------------
    project_dir <- normalizePath(".", winslash = "/", mustWork = TRUE)

# Runtime overrides ------------------------------------------------------------
    allow_windows_parallel <- TRUE
    parallel_cluster_type <- "PSOCK"

# Main process -----------------------------------------------------------------
    source(file.path(project_dir, "02_download", "07_fetch_no2_statistical_parallel_apac.R"))
