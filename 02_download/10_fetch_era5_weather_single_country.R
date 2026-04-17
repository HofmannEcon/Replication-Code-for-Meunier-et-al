# Fetch ERA5 timeseries weather data for a single country's cities from the CDS
# https://cds.climate.copernicus.eu/

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(httr2)
    library(jsonlite)
    library(lubridate)
    library(readr)
    library(zoo)

# Settings ---------------------------------------------------------------------
    # General settings
        project_dir <- "/Users/chrispohweitiong/Documents/Claude Codex Projects/9 Isolating NO2"

    # Specific settings
        target_country_iso3 <- "PHL"       # SGP, MYS, IDN, THA, PHL, VNM, IND, KOR, TWN, HKG, CHN, JPN, AUS, NZL
        target_country_name <- "Philippines"
        target_country_slug <- "philippines"

        full_run_start_date <- as.Date("2019-01-01")
        full_run_end_date <- as.Date("2026-04-08")
        batch_workers <- 4L
        batch_rounds_per_submission <- 3L
        request_delay_seconds <- 1
        overwrite_existing_files <- FALSE

        weather_region_slug <- target_country_slug
        weather_region_label <- target_country_name
        weather_raw_dir_label <- paste0("ERA5_", toupper(target_country_slug))
        run_label <- paste0(
            format(full_run_start_date, "%Y-%m"),
            "_to_",
            format(full_run_end_date, "%Y-%m")
        )

        city_buffer_path <- file.path(
            project_dir,
            "01_city_definitions",
            paste0(target_country_slug, "_cities_with_buffers.csv")
        )
        env_path <- file.path(project_dir, ".env")
        raw_output_dir <- file.path(
            project_dir,
            "raw_data",
            "weather",
            weather_raw_dir_label
        )
        request_manifest_path <- file.path(
            raw_output_dir,
            paste0("era5_request_manifest_", weather_region_slug, "_", run_label, ".csv")
        )
        download_log_path <- file.path(
            raw_output_dir,
            paste0("era5_download_log_", weather_region_slug, "_", run_label, ".csv")
        )

        era5_dataset <- "reanalysis-era5-single-levels-timeseries"
        era5_variables <- c(
            "2m_temperature",
            "2m_dewpoint_temperature",
            "total_precipitation",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "10m_wind_gust_since_previous_post_processing"
        )
        data_format <- "csv"

# Validation -------------------------------------------------------------------
    if (!file.exists(city_buffer_path)) {
        stop("City buffer file not found: ", city_buffer_path)
    }

    if (is.na(full_run_start_date) || is.na(full_run_end_date)) {
        stop("`--start-date` and `--end-date` must be valid dates in YYYY-MM-DD format.")
    }

    if (full_run_end_date < full_run_start_date) {
        stop("`--end-date` must be on or after `--start-date`.")
    }

    if (is.na(batch_workers) || batch_workers < 1L) {
        stop("`--workers` must be a positive integer.")
    }

    if (is.na(request_delay_seconds) || request_delay_seconds < 0) {
        stop("`--delay-seconds` must be a non-negative number.")
    }

    dir.create(raw_output_dir, recursive = TRUE, showWarnings = FALSE)

# Helper functions -------------------------------------------------------------
    read_env_file <- function(env_path) {
        if (!file.exists(env_path)) {
            return(list())
        }

        env_lines <- readLines(env_path, warn = FALSE)
        env_values <- list()

        for (env_line in env_lines) {
            trimmed_line <- trimws(env_line)

            if (!nzchar(trimmed_line) || startsWith(trimmed_line, "#")) {
                next
            }

            split_position <- regexpr("=", trimmed_line, fixed = TRUE)

            if (split_position[[1]] <= 0) {
                next
            }

            key_name <- trimws(substr(trimmed_line, 1, split_position[[1]] - 1))
            key_value <- trimws(substr(trimmed_line, split_position[[1]] + 1, nchar(trimmed_line)))

            if (
                nchar(key_value) >= 2 &&
                (
                    (startsWith(key_value, "\"") && endsWith(key_value, "\"")) ||
                    (startsWith(key_value, "'") && endsWith(key_value, "'"))
                )
            ) {
                key_value <- substr(key_value, 2, nchar(key_value) - 1)
            }

            env_values[[key_name]] <- key_value
        }

        env_values
    }

    sanitize_for_filename <- function(value) {
        sanitized_value <- gsub("[^A-Za-z0-9]+", "_", value)
        sanitized_value <- gsub("^_+|_+$", "", sanitized_value)
        tolower(sanitized_value)
    }

    resolve_downloaded_file_path <- function(raw_output_dir, target_file) {
        candidate_csv_path <- file.path(raw_output_dir, target_file)
        candidate_zip_path <- sub("\\.csv$", ".zip", candidate_csv_path)

        if (file.exists(candidate_csv_path)) {
            return(candidate_csv_path)
        }

        if (file.exists(candidate_zip_path)) {
            return(candidate_zip_path)
        }

        NA_character_
    }

    build_request_windows <- function(start_date, end_date) {
        years <- seq(year(start_date), year(end_date))
        window_rows <- vector("list", length = 0)

        for (window_year in years) {
            year_start <- max(start_date, as.Date(sprintf("%04d-01-01", window_year)))
            year_end <- min(end_date, as.Date(sprintf("%04d-12-31", window_year)))

            is_full_year <- identical(year_start, as.Date(sprintf("%04d-01-01", window_year))) &&
                identical(year_end, as.Date(sprintf("%04d-12-31", window_year)))

            if (is_full_year) {
                window_rows[[length(window_rows) + 1L]] <- tibble(
                    request_year = window_year,
                    request_start = year_start,
                    request_end = year_end,
                    request_label = as.character(window_year),
                    request_granularity = "year",
                    request_months = list(sprintf("%02d", 1:12)),
                    request_days = list(sprintf("%02d", 1:31))
                )
                next
            }

            month_starts <- seq(
                floor_date(year_start, unit = "month"),
                floor_date(year_end, unit = "month"),
                by = "1 month"
            )

            for (month_start in month_starts) {
                month_start <- as.Date(month_start, origin = "1970-01-01")
                month_window_start <- max(year_start, as.Date(month_start))
                month_window_end <- min(year_end, ceiling_date(month_start, unit = "month") - days(1))
                month_days <- seq(month_window_start, month_window_end, by = "1 day")

                window_rows[[length(window_rows) + 1L]] <- tibble(
                    request_year = window_year,
                    request_start = month_window_start,
                    request_end = month_window_end,
                    request_label = format(month_start, "%Y-%m"),
                    request_granularity = "month",
                    request_months = list(format(month_start, "%m")),
                    request_days = list(format(month_days, "%d"))
                )
            }
        }

        bind_rows(window_rows)
    }

    build_request_manifest <- function(cities, request_windows, raw_output_dir) {
        manifest_rows <- vector("list", length = 0)

        for (city_index in seq_len(nrow(cities))) {
            city_row <- cities[city_index, ]

            for (window_index in seq_len(nrow(request_windows))) {
                window_row <- request_windows[window_index, ]
                target_file <- paste0(
                    city_row$country_iso3,
                    "_",
                    sanitize_for_filename(city_row$city_name),
                    "_",
                    window_row$request_label,
                    ".csv"
                )
                dest_path <- file.path(raw_output_dir, target_file)

                manifest_rows[[length(manifest_rows) + 1L]] <- tibble(
                    country_iso3 = city_row$country_iso3,
                    country_name = city_row$country_name,
                    city_name = city_row$city_name,
                    latitude = city_row$latitude,
                    longitude = city_row$longitude,
                    request_year = window_row$request_year,
                    request_start = window_row$request_start,
                    request_end = window_row$request_end,
                    request_label = window_row$request_label,
                    request_granularity = window_row$request_granularity,
                    request_months = window_row$request_months,
                    request_days = window_row$request_days,
                    target_file = target_file,
                    dest_path = dest_path,
                    cached_before_run = !is.na(resolve_downloaded_file_path(raw_output_dir, target_file))
                )
            }
        }

        bind_rows(manifest_rows)
    }

    build_era5_request <- function(manifest_row) {
        list(
            inputs = list(
                variable = era5_variables,
                location = list(
                    longitude = manifest_row$longitude,
                    latitude = manifest_row$latitude
                ),
                date = paste0(
                    format(manifest_row$request_start, "%Y-%m-%d"),
                    "/",
                    format(manifest_row$request_end, "%Y-%m-%d")
                ),
                data_format = data_format
            ),
            target_file = manifest_row$target_file
        )
    }

    cds_api_request <- function(url, token, method = "GET", body = NULL) {
        request_object <- request(url) %>%
            req_headers(
                `PRIVATE-TOKEN` = token,
                `User-Agent` = "weather_step4_r"
            ) %>%
            req_method(method)

        if (!is.null(body)) {
            request_object <- request_object %>%
                req_body_json(body, auto_unbox = TRUE)
        }

        req_perform(request_object)
    }

    submit_cds_job <- function(request_body, token) {
        response <- cds_api_request(
            url = paste0(
                "https://cds.climate.copernicus.eu/api/retrieve/v1/processes/",
                era5_dataset,
                "/execution"
            ),
            token = token,
            method = "POST",
            body = request_body
        )

        jsonlite::fromJSON(resp_body_string(response), simplifyVector = FALSE)
    }

    poll_cds_job <- function(job_url, token, poll_interval_seconds = 2, max_polls = 1800L) {
        last_payload <- NULL

        for (poll_index in seq_len(max_polls)) {
            response <- cds_api_request(job_url, token = token, method = "GET")
            payload <- jsonlite::fromJSON(resp_body_string(response), simplifyVector = FALSE)
            last_payload <- payload

            if (payload$status %in% c("successful", "failed", "rejected")) {
                return(payload)
            }

            Sys.sleep(poll_interval_seconds)
        }

        stop("Timed out while polling CDS job status: ", job_url)
    }

    extract_results_href <- function(job_payload) {
        job_links <- job_payload$links

        if (is.null(job_links) || length(job_links) == 0) {
            return(NA_character_)
        }

        for (link_index in seq_along(job_links)) {
            if (identical(job_links[[link_index]]$rel, "results")) {
                return(job_links[[link_index]]$href)
            }
        }

        NA_character_
    }

    extract_asset_href <- function(results_payload) {
        asset_value <- results_payload$asset$value

        if (is.null(asset_value$href)) {
            return(NA_character_)
        }

        asset_value$href
    }

    download_cds_asset <- function(asset_href, target_file, token, raw_output_dir) {
        zip_target_path <- file.path(raw_output_dir, sub("\\.csv$", ".zip", target_file))

        request(asset_href) %>%
            req_headers(
                `PRIVATE-TOKEN` = token,
                `User-Agent` = "weather_step4_r"
            ) %>%
            req_perform(path = zip_target_path)

        zip_target_path
    }

# Prepare inputs ----------------------------------------------------------------
    env_values <- read_env_file(env_path)
    cds_api_key <- env_values[["CDS_API_KEY"]]

    if (is.null(cds_api_key) || !nzchar(cds_api_key)) {
        stop("CDS API key not available. Add `CDS_API_KEY=...` to `.env`.")
    }

    cities <- read_csv(city_buffer_path, show_col_types = FALSE) %>%
        select(country_iso3, country_name, city_name, latitude, longitude)

    request_windows <- build_request_windows(full_run_start_date, full_run_end_date)
    request_manifest <- build_request_manifest(cities, request_windows, raw_output_dir)

    write_csv(
        request_manifest %>%
            mutate(
                request_months = vapply(request_months, paste, character(1), collapse = ";"),
                request_days = vapply(request_days, paste, character(1), collapse = ";")
            ),
        request_manifest_path
    )

    message("=== ERA5 ", weather_region_label, " weather retrieval ===")
    message("Run label: ", run_label)
    message("Country ISO3: ", target_country_iso3)
    message("Cities: ", nrow(cities))
    message("Request windows: ", nrow(request_windows))
    message("Total city-window combinations: ", nrow(request_manifest))

# Download raw files -----------------------------------------------------------
    manifest_to_submit <- request_manifest %>%
        filter(overwrite_existing_files | !cached_before_run)

    download_log_rows <- list(
        request_manifest %>%
            transmute(
                country_iso3,
                country_name,
                city_name,
                request_label,
                request_granularity,
                target_file,
                status = ifelse(cached_before_run, "cached_before_run", "pending"),
                batch_number = NA_integer_,
                checked_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
            )
    )

    if (nrow(manifest_to_submit) > 0) {
        batch_size <- max(batch_workers * batch_rounds_per_submission, batch_workers)
        number_of_batches <- ceiling(nrow(manifest_to_submit) / batch_size)

        message("Requests to submit: ", nrow(manifest_to_submit))
        message("Batch workers: ", batch_workers)
        message("Estimated batches: ", number_of_batches)

        for (batch_number in seq_len(number_of_batches)) {
            start_index <- (batch_number - 1L) * batch_size + 1L
            end_index <- min(batch_number * batch_size, nrow(manifest_to_submit))
            batch_manifest <- manifest_to_submit[start_index:end_index, ]

            message(
                "\nSubmitting batch ", batch_number, "/", number_of_batches,
                " (", start_index, "-", end_index, ")"
            )

            batch_log_rows <- vector("list", length = nrow(batch_manifest))

            for (row_index in seq_len(nrow(batch_manifest))) {
                manifest_row <- batch_manifest[row_index, ]
                row_status <- "batch_failed"
                row_error_message <- NA_character_

                tryCatch(
                    {
                        request_payload <- build_era5_request(manifest_row)
                        submitted_job <- submit_cds_job(
                            request_body = request_payload,
                            token = cds_api_key
                        )
                        final_job <- poll_cds_job(
                            job_url = submitted_job$links[[2]]$href,
                            token = cds_api_key
                        )

                        if (identical(final_job$status, "successful")) {
                            results_href <- extract_results_href(final_job)
                            results_payload <- jsonlite::fromJSON(
                                resp_body_string(cds_api_request(results_href, token = cds_api_key)),
                                simplifyVector = FALSE
                            )
                            asset_href <- extract_asset_href(results_payload)

                            if (!is.na(asset_href)) {
                                download_cds_asset(
                                    asset_href = asset_href,
                                    target_file = manifest_row$target_file,
                                    token = cds_api_key,
                                    raw_output_dir = raw_output_dir
                                )
                                row_status <- "available_after_batch"
                            } else {
                                row_status <- "missing_after_batch"
                                row_error_message <- "Results payload did not contain an asset download URL."
                            }
                        } else {
                            row_status <- "batch_failed"
                            row_error_message <- paste0("CDS job ended with status: ", final_job$status)
                        }
                    },
                    error = function(e) {
                        row_status <<- "batch_failed"
                        row_error_message <<- conditionMessage(e)
                    }
                )

                if (!is.na(resolve_downloaded_file_path(raw_output_dir, manifest_row$target_file))) {
                    row_status <- "available_after_batch"
                }

                if (!is.na(row_error_message)) {
                    warning(
                        "Request failed for ", manifest_row$target_file, ": ",
                        row_error_message
                    )
                }

                batch_log_rows[[row_index]] <- tibble(
                    country_iso3 = manifest_row$country_iso3,
                    country_name = manifest_row$country_name,
                    city_name = manifest_row$city_name,
                    request_label = manifest_row$request_label,
                    request_granularity = manifest_row$request_granularity,
                    target_file = manifest_row$target_file,
                    status = row_status,
                    batch_number = batch_number,
                    checked_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
                )
            }

            download_log_rows[[length(download_log_rows) + 1L]] <- bind_rows(batch_log_rows)

            if (batch_number < number_of_batches && request_delay_seconds > 0) {
                message("Waiting ", request_delay_seconds, " seconds before the next batch...")
                Sys.sleep(request_delay_seconds)
            }
        }
    } else {
        message("All requested ERA5 files are already cached.")
    }

    write_csv(bind_rows(download_log_rows), download_log_path)

    message("\nSaved: ", request_manifest_path)
    message("Saved: ", download_log_path)
    message("\nStep 4 complete: raw ERA5 retrieval finished.")
