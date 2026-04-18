# Aggregate country-level version2 daily NO2 files to monthly averages

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(lubridate)
    library(readr)
    library(stringr)

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- "xxx"
        input_dir <- file.path(project_dir, "04_output", "data", "version2")
        output_dir <- input_dir

    # File pattern settings -----------------------------------------------------
        input_file_pattern <- "^no2_daily_country_(.+)_([0-9]{4}-[0-9]{2}_to_[0-9]{4}-[0-9]{2})_version2\\.csv$"
        output_file_template <- "no2_monthly_country_%s_%s_version2.csv"

# Validation -------------------------------------------------------------------
    if (!dir.exists(input_dir)) {
        stop("Input directory not found: ", input_dir)
    }

    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Helper functions -------------------------------------------------------------
    safe_mean <- function(x) {
        if (all(is.na(x))) {
            return(NA_real_)
        }

        mean(x, na.rm = TRUE)
    }

    build_monthly_output_path <- function(country_slug, date_stub) {
        file.path(
            output_dir,
            sprintf(output_file_template, country_slug, date_stub)
        )
    }

# Prepare input files ----------------------------------------------------------
    input_files <- list.files(
        input_dir,
        pattern = input_file_pattern,
        full.names = TRUE
    )

    if (length(input_files) == 0) {
        stop("No version2 daily country NO2 files were found in: ", input_dir)
    }

    input_manifest <- tibble(
        input_path = input_files,
        input_file = basename(input_files)
    ) %>%
        mutate(
            country_slug = str_match(input_file, input_file_pattern)[, 2],
            date_stub = str_match(input_file, input_file_pattern)[, 3]
        )

# Main process -----------------------------------------------------------------
    for (file_index in seq_len(nrow(input_manifest))) {
        manifest_row <- input_manifest[file_index, ]

        daily_country <- read_csv(manifest_row$input_path, show_col_types = FALSE) %>%
            mutate(date = as.Date(date))

        required_columns <- c(
            "country_iso3",
            "country_name",
            "date",
            "country_no2_index_daily",
            "country_no2_index_14dma",
            "country_no2_index_28dma"
        )
        missing_columns <- setdiff(required_columns, names(daily_country))

        if (length(missing_columns) > 0) {
            stop(
                "Missing required columns in `", manifest_row$input_file, "`: ",
                paste(missing_columns, collapse = ", ")
            )
        }

        monthly_country <- daily_country %>%
            mutate(date = floor_date(date, unit = "month")) %>%
            group_by(country_iso3, country_name, date) %>%
            summarise(
                country_no2_index_daily_average = safe_mean(country_no2_index_daily),
                country_no2_index_14dma_average = safe_mean(country_no2_index_14dma),
                country_no2_index_28dma_average = safe_mean(country_no2_index_28dma),
                .groups = "drop"
            ) %>%
            arrange(country_iso3, date)

        output_path <- build_monthly_output_path(
            country_slug = manifest_row$country_slug,
            date_stub = manifest_row$date_stub
        )

        write_csv(monthly_country, output_path)
        message("Saved: ", output_path)
    }

    message("Version2 daily-to-monthly NO2 aggregation complete.")
