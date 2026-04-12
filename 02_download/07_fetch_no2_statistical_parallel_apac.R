# Fetch daily NO2 statistics from the Sentinel Hub Statistical API in parallel for APAC

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(httr2)
    library(jsonlite)
    library(lubridate)
    library(parallel)
    library(readr)

# Runtime overrides ------------------------------------------------------------
    if (!exists("allow_windows_parallel")) {
        allow_windows_parallel <- FALSE
    }

    if (!exists("parallel_cluster_type")) {
        parallel_cluster_type <- "PSOCK"
    }

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- normalizePath(".", winslash = "/", mustWork = TRUE)
        full_run_start_date <- as.Date("2019-01-01")
        full_run_end_date <- as.Date("2019-12-31")
        requested_parallel_workers <- 4L
        request_delay_seconds <- 0.5
        run_label <- paste0(
            format(full_run_start_date, "%Y-%m"),
            "_to_",
            format(full_run_end_date, "%Y-%m")
        )

        city_buffer_path <- file.path(
            project_dir,
            "01_city_definitions",
            "apac_cities_with_buffers.csv"
        )
        env_path <- file.path(project_dir, ".env")
        auth_helper_path <- file.path(project_dir, "00_setup", "cdse_auth_helpers.R")
        raw_output_dir <- file.path(project_dir, "raw_data", "APAC", paste0("parallel_", run_label))
        download_log_path <- file.path(raw_output_dir, "no2_download_log_parallel.csv")

        token_url <- paste0(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/",
            "protocol/openid-connect/token"
        )
        statistical_api_url <- "https://sh.dataspace.copernicus.eu/api/v1/statistics"
        client_id <- "cdse-public"

        overwrite_existing_files <- FALSE
        max_retries <- 4
        retry_backoff_seconds <- 2
        token_refresh_margin_seconds <- 120
        stop_on_first_error <- FALSE

    # Specific settings ---------------------------------------------------------
        full_run_country_iso3 <- c(
            "AUS", "CHN", "HKG", "IDN", "IND", "JPN", "KOR",
            "MYS", "NZL", "PHL", "SGP", "THA", "TWN", "VNM"
        )

        sentinel_collection <- "sentinel-5p-l2"
        sentinel_product_type <- "NO2"
        sentinel_timeliness <- "OFFL"
        sentinel_min_qa <- 75
        aggregation_interval <- "P1D"
        resx_degrees <- 0.05
        resy_degrees <- 0.05
        evalscript <- paste(
            "//VERSION=3",
            "function setup() {",
            "  return {",
            "    input: [{ bands: [\"NO2\", \"dataMask\"] }],",
            "    output: [",
            "      { id: \"no2\", bands: 1, sampleType: \"FLOAT32\" },",
            "      { id: \"dataMask\", bands: 1, sampleType: \"UINT8\" }",
            "    ]",
            "  };",
            "}",
            "function evaluatePixel(sample) {",
            "  return {",
            "    no2: [sample.NO2],",
            "    dataMask: [sample.dataMask]",
            "  };",
            "}",
            sep = "\n"
        )

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
        requested_parallel_workers <- as.integer(
            get_script_arg("workers", as.character(requested_parallel_workers))
        )
        request_delay_seconds <- as.numeric(
            get_script_arg("delay-seconds", as.character(request_delay_seconds))
        )
        run_label <- paste0(
            format(full_run_start_date, "%Y-%m"),
            "_to_",
            format(full_run_end_date, "%Y-%m")
        )
        raw_output_dir <- file.path(project_dir, "raw_data", "APAC", paste0("parallel_", run_label))
        download_log_path <- file.path(raw_output_dir, "no2_download_log_parallel.csv")
    }

# Validation -------------------------------------------------------------------
    if (!file.exists(city_buffer_path)) {
        stop("APAC city buffer file not found at `01_city_definitions/apac_cities_with_buffers.csv`.")
    }

    if (is.na(full_run_start_date) || is.na(full_run_end_date)) {
        stop("`--start-date` and `--end-date` must be valid dates in YYYY-MM-DD format.")
    }

    if (full_run_end_date < full_run_start_date) {
        stop("`--end-date` must be on or after `--start-date`.")
    }

    if (is.na(requested_parallel_workers) || requested_parallel_workers < 1L) {
        stop("`--workers` must be a positive integer.")
    }

    if (is.na(request_delay_seconds) || request_delay_seconds < 0) {
        stop("`--delay-seconds` must be a non-negative number.")
    }

    if (!file.exists(env_path)) {
        stop("`.env` file not found in the project root.")
    }

    if (!file.exists(auth_helper_path)) {
        stop("Authentication helper file not found at `00_setup/cdse_auth_helpers.R`.")
    }

    if (.Platform$OS.type == "windows" && !isTRUE(allow_windows_parallel)) {
        stop("This parallel Step 4 APAC script uses forked workers and is intended for macOS or Linux.")
    }

    dir.create(raw_output_dir, recursive = TRUE, showWarnings = FALSE)

# Prepare raw data -------------------------------------------------------------
    source(auth_helper_path)

    city_buffers <- read_csv(city_buffer_path, show_col_types = FALSE)

    if (!"geometry_geojson" %in% names(city_buffers)) {
        stop("`geometry_geojson` column is missing from the APAC city buffer file.")
    }

# Helper functions -------------------------------------------------------------
    `%||%` <- function(x, y) {
        if (is.null(x)) y else x
    }

    sanitize_for_filename <- function(value) {
        sanitized_value <- gsub("[^A-Za-z0-9]+", "_", value)
        sanitized_value <- gsub("^_+|_+$", "", sanitized_value)
        tolower(sanitized_value)
    }

    build_month_windows <- function(start_date, end_date) {
        month_starts <- seq(
            floor_date(start_date, unit = "month"),
            floor_date(end_date, unit = "month"),
            by = "1 month"
        )

        tibble(
            month_start = month_starts,
            month_end = pmin(ceiling_date(month_starts, unit = "month") - days(1), end_date),
            month_label = format(month_starts, "%Y-%m")
        ) %>%
            filter(month_end >= start_date)
    }

    build_timestamp_range <- function(start_date, end_date) {
        exclusive_end_date <- end_date + days(1)

        list(
            from = paste0(format(start_date, "%Y-%m-%d"), "T00:00:00Z"),
            to = paste0(format(exclusive_end_date, "%Y-%m-%d"), "T00:00:00Z")
        )
    }

    initialize_auth_state <- function(env_path, token_url, client_id) {
        token_body <- get_cdse_access_token(
            env_path = env_path,
            token_url = token_url,
            client_id = client_id
        )

        list(
            access_token = token_body$access_token,
            expires_in = as.numeric(token_body$expires_in),
            fetched_at = Sys.time()
        )
    }

    refresh_auth_state_if_needed <- function(auth_state,
                                             env_path,
                                             token_url,
                                             client_id,
                                             refresh_margin_seconds,
                                             force_refresh = FALSE) {
        token_age_seconds <- as.numeric(difftime(Sys.time(), auth_state$fetched_at, units = "secs"))
        refresh_after_seconds <- max(0, auth_state$expires_in - refresh_margin_seconds)

        if (force_refresh || token_age_seconds >= refresh_after_seconds) {
            message("Refreshing CDSE access token.")

            return(initialize_auth_state(
                env_path = env_path,
                token_url = token_url,
                client_id = client_id
            ))
        }

        auth_state
    }

    build_request_body <- function(city_row, month_row) {
        time_range <- build_timestamp_range(month_row$month_start, month_row$month_end)
        geometry_object <- fromJSON(city_row$geometry_geojson, simplifyVector = FALSE)

        data_entry <- list(
            type = sentinel_collection,
            dataFilter = list(
                timeliness = sentinel_timeliness
            ),
            processing = list(
                minQa = sentinel_min_qa
            )
        )
        data_entry[["s5p:type"]] <- sentinel_product_type

        list(
            input = list(
                bounds = list(
                    geometry = geometry_object,
                    properties = list(crs = "http://www.opengis.net/def/crs/EPSG/0/4326")
                ),
                data = list(data_entry)
            ),
            aggregation = list(
                timeRange = time_range,
                aggregationInterval = list(of = aggregation_interval),
                width = NULL,
                height = NULL,
                resx = resx_degrees,
                resy = resy_degrees,
                evalscript = evalscript
            )
        )
    }

    extract_daily_results <- function(response_body) {
        response_data <- response_body$data %||% list()

        if (length(response_data) == 0) {
            return(tibble())
        }

        tibble(
            date = as.Date(vapply(
                response_data,
                function(entry) substr(entry$interval$from %||% NA_character_, 1, 10),
                character(1)
            )),
            no2_mean_mol_m2 = vapply(
                response_data,
                function(entry) {
                    entry$output$no2$bands$B0$stats$mean %||% NA_real_
                },
                numeric(1)
            ),
            data_mask_mean = vapply(
                response_data,
                function(entry) {
                    entry$output$dataMask$bands$B0$stats$mean %||% NA_real_
                },
                numeric(1)
            ),
            sample_count = vapply(
                response_data,
                function(entry) {
                    entry$output$no2$bands$B0$stats$sampleCount %||% NA_real_
                },
                numeric(1)
            ),
            no_data_count = vapply(
                response_data,
                function(entry) {
                    entry$output$no2$bands$B0$stats$noDataCount %||% NA_real_
                },
                numeric(1)
            )
        ) %>%
            mutate(
                date = as.Date(date),
                has_data = !is.na(sample_count) & sample_count > 0,
                no2_mean_mol_m2 = if_else(has_data, no2_mean_mol_m2, NA_real_)
            )
    }

    city_month_output_path <- function(city_row, month_label) {
        filename <- paste0(
            city_row$country_iso3,
            "_",
            sanitize_for_filename(city_row$city_name),
            "_",
            month_label,
            ".csv"
        )

        file.path(raw_output_dir, filename)
    }

    perform_statistical_request <- function(city_row,
                                            month_row,
                                            auth_state,
                                            env_path,
                                            token_url,
                                            client_id,
                                            refresh_margin_seconds,
                                            api_url,
                                            max_retries,
                                            retry_backoff_seconds) {
        auth_state <- refresh_auth_state_if_needed(
            auth_state = auth_state,
            env_path = env_path,
            token_url = token_url,
            client_id = client_id,
            refresh_margin_seconds = refresh_margin_seconds
        )

        request_body <- build_request_body(city_row, month_row)

        for (attempt_index in seq_len(max_retries)) {
            request_object <- request(api_url) |>
                req_method("POST") |>
                req_headers(
                    Authorization = paste("Bearer", auth_state$access_token),
                    `Content-Type` = "application/json"
                ) |>
                req_body_json(request_body, auto_unbox = TRUE)

            response <- tryCatch(
                req_perform(request_object),
                error = function(error_object) error_object
            )

            if (!inherits(response, "error")) {
                response_body <- resp_body_json(response, simplifyVector = FALSE)

                return(list(
                    auth_state = auth_state,
                    response_body = response_body
                ))
            }

            response_text <- conditionMessage(response)

            if (grepl("401", response_text, fixed = TRUE)) {
                auth_state <- refresh_auth_state_if_needed(
                    auth_state = auth_state,
                    env_path = env_path,
                    token_url = token_url,
                    client_id = client_id,
                    refresh_margin_seconds = refresh_margin_seconds,
                    force_refresh = TRUE
                )
            } else if (attempt_index < max_retries) {
                Sys.sleep(retry_backoff_seconds * attempt_index)
            } else {
                stop(response_text)
            }
        }
    }

    build_log_row <- function(worker_id,
                              city_row,
                              month_row,
                              output_path,
                              status,
                              message_text = "") {
        tibble(
            timestamp_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
            worker_id = worker_id,
            country_iso3 = city_row$country_iso3,
            city_name = city_row$city_name,
            request_month = month_row$month_label,
            status = status,
            output_file = basename(output_path),
            message = message_text
        )
    }

    get_worker_state_env <- function() {
        state_name <- ".no2_parallel_worker_state"

        if (!exists(state_name, envir = .GlobalEnv, inherits = FALSE)) {
            assign(state_name, new.env(parent = emptyenv()), envir = .GlobalEnv)
        }

        get(state_name, envir = .GlobalEnv, inherits = FALSE)
    }

    get_worker_id <- function() {
        state_env <- get_worker_state_env()

        if (!exists("worker_id", envir = state_env, inherits = FALSE)) {
            assign("worker_id", paste0("worker_pid_", Sys.getpid()), envir = state_env)
        }

        get("worker_id", envir = state_env, inherits = FALSE)
    }

    get_worker_auth_state <- function() {
        state_env <- get_worker_state_env()

        if (!exists("auth_state", envir = state_env, inherits = FALSE)) {
            assign(
                "auth_state",
                initialize_auth_state(
                    env_path = env_path,
                    token_url = token_url,
                    client_id = client_id
                ),
                envir = state_env
            )
        }

        get("auth_state", envir = state_env, inherits = FALSE)
    }

    set_worker_auth_state <- function(auth_state) {
        state_env <- get_worker_state_env()
        assign("auth_state", auth_state, envir = state_env)
        invisible(auth_state)
    }

    process_job <- function(job_index) {
        job_row <- job_table[job_index, , drop = FALSE]
        city_row <- scoped_city_buffers[job_row$city_index, , drop = FALSE]
        month_row <- month_windows[job_row$month_index, , drop = FALSE]
        worker_id <- get_worker_id()
        output_path <- city_month_output_path(city_row, month_row$month_label)

        if (!overwrite_existing_files && file.exists(output_path)) {
            return(build_log_row(
                worker_id = worker_id,
                city_row = city_row,
                month_row = month_row,
                output_path = output_path,
                status = "skipped_existing"
            ))
        }

        auth_state <- get_worker_auth_state()

        tryCatch(
            {
                request_result <- perform_statistical_request(
                    city_row = city_row,
                    month_row = month_row,
                    auth_state = auth_state,
                    env_path = env_path,
                    token_url = token_url,
                    client_id = client_id,
                    refresh_margin_seconds = token_refresh_margin_seconds,
                    api_url = statistical_api_url,
                    max_retries = max_retries,
                    retry_backoff_seconds = retry_backoff_seconds
                )

                auth_state <- request_result$auth_state
                set_worker_auth_state(auth_state)

                daily_results <- extract_daily_results(request_result$response_body) %>%
                    mutate(
                        country_iso3 = city_row$country_iso3,
                        country_name = city_row$country_name,
                        city_name = city_row$city_name,
                        month_label = month_row$month_label
                    ) %>%
                    select(
                        country_iso3,
                        country_name,
                        city_name,
                        month_label,
                        date,
                        no2_mean_mol_m2,
                        data_mask_mean,
                        sample_count,
                        no_data_count,
                        has_data
                    )

                write_csv(daily_results, output_path)

                if (request_delay_seconds > 0) {
                    Sys.sleep(request_delay_seconds)
                }

                status <- if (nrow(daily_results) == 0) "success_empty" else "success"

                build_log_row(
                    worker_id = worker_id,
                    city_row = city_row,
                    month_row = month_row,
                    output_path = output_path,
                    status = status
                )
            },
            error = function(error_object) {
                log_row <- build_log_row(
                    worker_id = worker_id,
                    city_row = city_row,
                    month_row = month_row,
                    output_path = output_path,
                    status = "failed",
                    message_text = conditionMessage(error_object)
                )

                if (stop_on_first_error) {
                    stop(conditionMessage(error_object))
                }

                log_row
            }
        )
    }

# Main process -----------------------------------------------------------------
    month_windows <- build_month_windows(full_run_start_date, full_run_end_date)

    scoped_city_buffers <- city_buffers %>%
        filter(country_iso3 %in% full_run_country_iso3)

    if (nrow(scoped_city_buffers) == 0) {
        stop("No APAC cities matched the current parallel Step 4 settings.")
    }

    job_table <- expand.grid(
        city_index = seq_len(nrow(scoped_city_buffers)),
        month_index = seq_len(nrow(month_windows))
    ) %>%
        as_tibble() %>%
        mutate(job_id = row_number()) %>%
        arrange(job_id)

    total_requests <- nrow(job_table)
    available_physical_cores <- parallel::detectCores(logical = FALSE)

    if (is.na(available_physical_cores) || available_physical_cores < 1L) {
        available_physical_cores <- 1L
    }

    parallel_workers <- max(
        1L,
        min(
            requested_parallel_workers,
            nrow(job_table),
            available_physical_cores
        )
    )

    if (parallel_workers == 1L) {
        worker_results <- lapply(seq_len(nrow(job_table)), process_job)
    } else {
        cluster_object <- parallel::makeCluster(
            parallel_workers,
            type = parallel_cluster_type,
            outfile = ""
        )

        on.exit(parallel::stopCluster(cluster_object), add = TRUE)

        parallel::clusterExport(
            cluster_object,
            varlist = c("auth_helper_path"),
            envir = environment()
        )

        parallel::clusterEvalQ(
            cluster_object,
            {
                library(dplyr)
                library(httr2)
                library(jsonlite)
                library(lubridate)
                library(readr)
                source(auth_helper_path)
            }
        )

        parallel::clusterExport(
            cluster_object,
            varlist = c(
                "%||%",
                "aggregation_interval",
                "build_log_row",
                "build_month_windows",
                "build_request_body",
                "build_timestamp_range",
                "city_month_output_path",
                "client_id",
                "env_path",
                "evalscript",
                "extract_daily_results",
                "full_run_country_iso3",
                "get_worker_auth_state",
                "get_worker_id",
                "get_worker_state_env",
                "initialize_auth_state",
                "job_table",
                "max_retries",
                "month_windows",
                "overwrite_existing_files",
                "perform_statistical_request",
                "process_job",
                "raw_output_dir",
                "refresh_auth_state_if_needed",
                "request_delay_seconds",
                "requested_parallel_workers",
                "resx_degrees",
                "resy_degrees",
                "retry_backoff_seconds",
                "sanitize_for_filename",
                "scoped_city_buffers",
                "sentinel_collection",
                "sentinel_min_qa",
                "sentinel_product_type",
                "set_worker_auth_state",
                "sentinel_timeliness",
                "statistical_api_url",
                "stop_on_first_error",
                "token_refresh_margin_seconds",
                "token_url"
            ),
            envir = environment()
        )

        worker_results <- parallel::parLapplyLB(
            cluster_object,
            seq_len(nrow(job_table)),
            process_job
        )
    }

    empty_download_logs <- tibble(
        timestamp_utc = character(),
        worker_id = character(),
        country_iso3 = character(),
        city_name = character(),
        request_month = character(),
        status = character(),
        output_file = character(),
        message = character()
    )

    download_logs <- bind_rows(empty_download_logs, bind_rows(worker_results)) %>%
        arrange(timestamp_utc, worker_id, country_iso3, city_name, request_month)

    if (nrow(download_logs) > 0) {
        write.table(
            download_logs,
            file = download_log_path,
            sep = ",",
            row.names = FALSE,
            col.names = !file.exists(download_log_path),
            append = file.exists(download_log_path),
            qmethod = "double"
        )
    }

    successful_requests <- sum(download_logs$status %in% c("success", "success_empty"))
    skipped_requests <- sum(download_logs$status == "skipped_existing")
    failed_requests <- sum(download_logs$status == "failed")

    message("APAC parallel Step 4 request window: ", full_run_start_date, " to ", full_run_end_date)
    message("Parallel workers used: ", parallel_workers)
    message("Total request slots in scope: ", total_requests)
    message("Successful requests: ", successful_requests)
    message("Skipped requests: ", skipped_requests)
    message("Failed requests: ", failed_requests)
    message("APAC parallel raw output directory: ", raw_output_dir)
    message("APAC parallel download log: ", download_log_path)
