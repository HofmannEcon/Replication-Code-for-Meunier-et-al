# Clean and aggregate dated APAC NO2 backfill data

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(lubridate)
    library(readr)
    library(tidyr)
    library(zoo)

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- normalizePath(".", winslash = "/", mustWork = TRUE)
        full_run_start_date <- as.Date("2019-01-01")
        full_run_end_date <- as.Date("2019-12-31")
        run_label <- paste0(
            format(full_run_start_date, "%Y-%m"),
            "_to_",
            format(full_run_end_date, "%Y-%m")
        )

        raw_data_dir <- file.path(project_dir, "raw_data", "APAC", paste0("parallel_", run_label))
        output_data_dir <- file.path(project_dir, "04_output", "data")
        download_log_path <- file.path(raw_data_dir, "no2_download_log_parallel.csv")
        city_output_path <- file.path(
            output_data_dir,
            paste0("no2_daily_city_apac_", run_label, "_backfill.csv")
        )
        daily_country_output_path <- file.path(
            output_data_dir,
            paste0("no2_daily_country_apac_", run_label, "_backfill.csv")
        )
        monthly_country_output_path <- file.path(
            output_data_dir,
            paste0("no2_monthly_country_apac_", run_label, "_backfill.csv")
        )

    # Processing settings -------------------------------------------------------
        raw_file_pattern <- "\\.csv$"
        excluded_raw_files <- c("no2_download_log_parallel.csv")
        rolling_window_days <- 28
        export_city_output <- TRUE
        negative_no2_treated_as_missing <- TRUE

# Optional command-line overrides ----------------------------------------------
    if (!interactive() || exists("script_args")) {
        if (!exists("script_args")) {
            script_args <- commandArgs(trailingOnly = TRUE)
        }

        get_script_arg <- function(name, default_value = NULL) {
            argument_prefix <- paste0("--", name, "=")
            matching_argument <- script_args[startsWith(script_args, argument_prefix)]

            if (length(matching_argument) == 0) {
                return(default_value)
            }

            sub(argument_prefix, "", matching_argument[[1]], fixed = TRUE)
        }

        full_run_start_date <- as.Date(
            get_script_arg("start-date", format(full_run_start_date, "%Y-%m-%d"))
        )
        full_run_end_date <- as.Date(
            get_script_arg("end-date", format(full_run_end_date, "%Y-%m-%d"))
        )
        run_label <- paste0(
            format(full_run_start_date, "%Y-%m"),
            "_to_",
            format(full_run_end_date, "%Y-%m")
        )
        raw_data_dir <- file.path(project_dir, "raw_data", "APAC", paste0("parallel_", run_label))
        download_log_path <- file.path(raw_data_dir, "no2_download_log_parallel.csv")
        city_output_path <- file.path(
            output_data_dir,
            paste0("no2_daily_city_apac_", run_label, "_backfill.csv")
        )
        daily_country_output_path <- file.path(
            output_data_dir,
            paste0("no2_daily_country_apac_", run_label, "_backfill.csv")
        )
        monthly_country_output_path <- file.path(
            output_data_dir,
            paste0("no2_monthly_country_apac_", run_label, "_backfill.csv")
        )
    }

# Validation -------------------------------------------------------------------
    if (is.na(full_run_start_date) || is.na(full_run_end_date)) {
        stop("`--start-date` and `--end-date` must be valid dates in YYYY-MM-DD format.")
    }

    if (full_run_end_date < full_run_start_date) {
        stop("`--end-date` must be on or after `--start-date`.")
    }

    if (!dir.exists(raw_data_dir)) {
        stop("APAC raw data directory not found at `", raw_data_dir, "`.")
    }

    if (!file.exists(download_log_path)) {
        stop("APAC download log not found at `", download_log_path, "`.")
    }

    raw_file_paths <- list.files(
        raw_data_dir,
        pattern = raw_file_pattern,
        full.names = TRUE
    )

    raw_file_paths <- raw_file_paths[!basename(raw_file_paths) %in% excluded_raw_files]

    if (length(raw_file_paths) == 0) {
        stop("No APAC city-month CSV files were found in `", raw_data_dir, "`.")
    }

    dir.create(output_data_dir, recursive = TRUE, showWarnings = FALSE)

# Prepare raw data -------------------------------------------------------------
    download_log <- read_csv(download_log_path, show_col_types = FALSE)

    failed_downloads <- download_log %>%
        filter(status == "failed")

    read_city_month_file <- function(file_path) {
        read_csv(file_path, show_col_types = FALSE)
    }

    raw_city_daily <- bind_rows(lapply(raw_file_paths, read_city_month_file))

    if (nrow(raw_city_daily) == 0) {
        stop("The APAC raw city-month files were found, but they contained no rows.")
    }

    required_columns <- c(
        "country_iso3",
        "country_name",
        "city_name",
        "un_city_code",
        "date",
        "mean_no2_mol_m2",
        "sample_count",
        "no_data_count",
        "valid_pixel_count"
    )

    missing_columns <- setdiff(required_columns, names(raw_city_daily))

    if (length(missing_columns) > 0) {
        stop(
            "APAC raw city data is missing required columns: ",
            paste(missing_columns, collapse = ", ")
        )
    }

    city_daily_clean <- raw_city_daily %>%
        transmute(
            country_iso3 = country_iso3,
            country_name = country_name,
            city_name = city_name,
            un_city_code = as.numeric(un_city_code),
            date = as.Date(date),
            mean_no2 = as.numeric(mean_no2_mol_m2),
            valid_pixels = as.numeric(valid_pixel_count),
            total_pixels = as.numeric(sample_count),
            no_data_pixels = as.numeric(no_data_count),
            data_coverage = if_else(total_pixels > 0, valid_pixels / total_pixels, NA_real_)
        ) %>%
        mutate(
            mean_no2 = if_else(is.na(valid_pixels) | valid_pixels <= 0, NA_real_, mean_no2),
            mean_no2 = if_else(
                negative_no2_treated_as_missing & !is.na(mean_no2) & mean_no2 < 0,
                NA_real_,
                mean_no2
            )
        ) %>%
        arrange(country_iso3, country_name, city_name, date)

    duplicate_city_dates <- city_daily_clean %>%
        count(country_iso3, country_name, city_name, un_city_code, date, name = "row_count") %>%
        filter(row_count > 1)

    if (nrow(duplicate_city_dates) > 0) {
        stop("Duplicate city-date rows were found in the APAC Step 4 outputs.")
    }

    city_daily_complete <- city_daily_clean %>%
        group_by(country_iso3, country_name, city_name, un_city_code) %>%
        complete(date = seq(min(date), max(date), by = "day")) %>%
        arrange(date, .by_group = TRUE) %>%
        mutate(
            mean_no2 = if_else(is.na(valid_pixels) | valid_pixels <= 0, NA_real_, mean_no2),
            mean_no2 = if_else(
                negative_no2_treated_as_missing & !is.na(mean_no2) & mean_no2 < 0,
                NA_real_,
                mean_no2
            )
        ) %>%
        mutate(
            mean_no2_28dma = zoo::rollapplyr(
                data = mean_no2,
                width = rolling_window_days,
                FUN = function(x) {
                    if (all(is.na(x))) {
                        return(NA_real_)
                    }

                    mean(x, na.rm = TRUE)
                },
                partial = TRUE,
                fill = NA_real_
            ),
            valid_days_in_window = zoo::rollapplyr(
                data = mean_no2,
                width = rolling_window_days,
                FUN = function(x) sum(!is.na(x)),
                partial = TRUE,
                fill = NA_integer_
            )
        ) %>%
        ungroup() %>%
        arrange(country_iso3, country_name, city_name, date)

# Helper functions -------------------------------------------------------------
    safe_mean <- function(x) {
        if (all(is.na(x))) {
            return(NA_real_)
        }

        mean(x, na.rm = TRUE)
    }

# Main process -----------------------------------------------------------------
    daily_country <- city_daily_complete %>%
        group_by(country_iso3, country_name, date) %>%
        summarise(
            country_no2_index = safe_mean(mean_no2_28dma),
            n_cities_total = n(),
            n_cities_with_value = sum(!is.na(mean_no2_28dma)),
            average_data_coverage = safe_mean(data_coverage),
            .groups = "drop"
        ) %>%
        arrange(country_iso3, date)

    monthly_country <- daily_country %>%
        mutate(month = floor_date(date, unit = "month")) %>%
        group_by(country_iso3, country_name, month) %>%
        summarise(
            n_days_total = n(),
            n_days_with_value = sum(!is.na(country_no2_index)),
            country_no2_index = safe_mean(country_no2_index),
            average_cities_with_value = safe_mean(n_cities_with_value),
            average_data_coverage = safe_mean(average_data_coverage),
            .groups = "drop"
        ) %>%
        arrange(country_iso3, month)

# Export data ------------------------------------------------------------------
    if (export_city_output) {
        write_csv(city_daily_complete, city_output_path, na = "")
    }

    write_csv(daily_country, daily_country_output_path, na = "")
    write_csv(monthly_country, monthly_country_output_path, na = "")

# Diagnostics ------------------------------------------------------------------
    total_cities_processed <- city_daily_complete %>%
        distinct(country_iso3, city_name) %>%
        nrow()

    total_missing_city_days <- city_daily_complete %>%
        filter(is.na(mean_no2)) %>%
        nrow()

    total_negative_values_removed <- raw_city_daily %>%
        summarise(negative_values_removed = sum(!is.na(mean_no2_mol_m2) & mean_no2_mol_m2 < 0)) %>%
        pull(negative_values_removed)

    city_date_range <- city_daily_complete %>%
        summarise(
            start_date = min(date, na.rm = TRUE),
            end_date = max(date, na.rm = TRUE)
        )

    message("APAC Step 5 processing complete for ", run_label, ".")
    message("Raw city-month files read: ", length(raw_file_paths))
    message("Failed download log entries found: ", nrow(failed_downloads))
    message("Cities processed: ", total_cities_processed)
    message("Date range covered: ", city_date_range$start_date, " to ", city_date_range$end_date)
    message("Negative city-day NO2 values treated as missing: ", total_negative_values_removed)
    message("Missing or no-coverage city-days: ", total_missing_city_days)
    message("Daily country rows written: ", nrow(daily_country))
    message("Monthly country rows written: ", nrow(monthly_country))
    message("Daily country output: ", daily_country_output_path)
    message("Monthly country output: ", monthly_country_output_path)

    if (export_city_output) {
        message("Daily city output: ", city_output_path)
    }
