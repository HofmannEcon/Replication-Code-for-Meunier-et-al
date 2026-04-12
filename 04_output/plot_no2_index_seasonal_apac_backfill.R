# Plot seasonal day-of-year NO2 overlays for dated APAC backfill data

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(ggplot2)
    library(readr)

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- normalizePath(".", winslash = "/", mustWork = TRUE)
        output_dir <- file.path(project_dir, "04_output")
        data_dir <- file.path(output_dir, "data")
        charts_dir <- file.path(output_dir, "charts")

        full_run_start_date <- as.Date("2019-01-01")
        full_run_end_date <- as.Date("2019-12-31")
        run_label <- paste0(
            format(full_run_start_date, "%Y-%m"),
            "_to_",
            format(full_run_end_date, "%Y-%m")
        )

        daily_country_input_path <- file.path(
            data_dir,
            paste0("no2_daily_country_apac_", run_label, "_backfill.csv")
        )
        output_file_stub <- "no2_daily_country_seasonal_overlay_apac"

    # Plot settings -------------------------------------------------------------
        country_order_iso3 <- c(
            "AUS", "CHN", "HKG", "IDN", "IND", "JPN", "KOR",
            "MYS", "NZL", "PHL", "SGP", "THA", "TWN", "VNM"
        )
        country_file_labels <- c(
            "AUS" = "aus",
            "CHN" = "chn",
            "HKG" = "hkg",
            "IDN" = "idn",
            "IND" = "ind",
            "JPN" = "jpn",
            "KOR" = "kor",
            "MYS" = "mys",
            "NZL" = "nzl",
            "PHL" = "phl",
            "SGP" = "sgp",
            "THA" = "tha",
            "TWN" = "twn",
            "VNM" = "vnm"
        )

        chart_width_inches <- 12
        chart_height_inches <- 6.8
        chart_dpi <- 300

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
        daily_country_input_path <- file.path(
            data_dir,
            paste0("no2_daily_country_apac_", run_label, "_backfill.csv")
        )
    }

# Validation -------------------------------------------------------------------
    if (is.na(full_run_start_date) || is.na(full_run_end_date)) {
        stop("`--start-date` and `--end-date` must be valid dates in YYYY-MM-DD format.")
    }

    if (full_run_end_date < full_run_start_date) {
        stop("`--end-date` must be on or after `--start-date`.")
    }

    if (!file.exists(daily_country_input_path)) {
        stop(
            "APAC daily country Step 5 output not found at `",
            daily_country_input_path,
            "`."
        )
    }

    dir.create(charts_dir, recursive = TRUE, showWarnings = FALSE)

# Prepare plotting data --------------------------------------------------------
    daily_country <- read_csv(daily_country_input_path, show_col_types = FALSE) %>%
        mutate(
            date = as.Date(date),
            year = format(date, "%Y"),
            plot_date = as.Date(paste0("2020-", format(date, "%m-%d")))
        ) %>%
        filter(!is.na(plot_date), !is.na(year)) %>%
        arrange(country_iso3, year, date)

    missing_countries <- setdiff(country_order_iso3, unique(as.character(daily_country$country_iso3)))

    if (length(missing_countries) > 0) {
        stop(
            "One or more APAC economies are missing from the seasonal plotting input: ",
            paste(missing_countries, collapse = ", ")
        )
    }

    year_levels <- sort(unique(as.character(daily_country$year)))
    latest_year <- tail(year_levels, 1)

    year_colour_map <- setNames(rep("#b8b8b8", length(year_levels)), year_levels)
    year_linewidth_map <- setNames(rep(0.4, length(year_levels)), year_levels)

    if ("2019" %in% year_levels) {
        year_colour_map["2019"] <- "#d9d9d9"
    }

    if ("2020" %in% year_levels) {
        year_colour_map["2020"] <- "#355070"
        year_linewidth_map["2020"] <- 0.9
    }

    if ("2025" %in% year_levels) {
        year_colour_map["2025"] <- "#7f7f7f"
        year_linewidth_map["2025"] <- 0.5
    }

    year_colour_map[latest_year] <- "#b23a48"
    year_linewidth_map[latest_year] <- 1.1

    daily_country <- daily_country %>%
        mutate(
            country_iso3 = factor(country_iso3, levels = country_order_iso3),
            year = factor(year, levels = year_levels)
        )

# Helper functions -------------------------------------------------------------
    build_base_theme <- function() {
        theme_minimal(base_size = 12) +
            theme(
                panel.grid.minor = element_blank(),
                panel.grid.major.x = element_blank(),
                plot.title = element_text(face = "bold"),
                axis.title.x = element_blank(),
                legend.position = "bottom",
                legend.title = element_blank()
            )
    }

    build_country_chart <- function(country_iso3_value) {
        country_data <- daily_country %>%
            filter(country_iso3 == country_iso3_value)

        country_label <- unique(as.character(country_data$country_name))

        if (length(country_label) != 1) {
            stop("Expected exactly one country name for ISO3 `", country_iso3_value, "`.")
        }

        ggplot(
            country_data,
            aes(
                x = plot_date,
                y = country_no2_index,
                colour = year,
                linewidth = year,
                group = year
            )
        ) +
            geom_line(na.rm = TRUE) +
            scale_colour_manual(values = year_colour_map, drop = FALSE) +
            scale_linewidth_manual(values = year_linewidth_map, drop = FALSE) +
            scale_x_date(
                date_breaks = "1 month",
                date_labels = "%b",
                limits = as.Date(c("2020-01-01", "2020-12-31"))
            ) +
            scale_y_continuous(labels = scales::label_scientific(digits = 2)) +
            labs(
                title = paste0(country_label, ": Seasonal Daily NO2 Overlay"),
                subtitle = paste(
                    "Daily country NO2 index shown on a Jan-Dec calendar axis.",
                    "2020 is highlighted as the COVID benchmark,",
                    "and the latest available year is emphasized as the current comparison line."
                ),
                y = "Tropospheric NO2 (mol/m^2)",
                caption = paste0(
                    "Source: APAC dated Sentinel-5P OFFL backfill Step 5 output (",
                    run_label,
                    ")."
                )
            ) +
            guides(linewidth = "none") +
            build_base_theme()
    }

# Main process -----------------------------------------------------------------
    for (country_iso3_value in country_order_iso3) {
        chart_object <- build_country_chart(country_iso3_value)

        output_path <- file.path(
            charts_dir,
            paste0(
                output_file_stub,
                "_",
                country_file_labels[[country_iso3_value]],
                "_",
                run_label,
                "_backfill.png"
            )
        )

        ggsave(
            filename = output_path,
            plot = chart_object,
            width = chart_width_inches,
            height = chart_height_inches,
            dpi = chart_dpi
        )

        message("APAC seasonal overlay chart: ", output_path)
    }

    message("APAC seasonal overlay Step 6 plotting complete.")
