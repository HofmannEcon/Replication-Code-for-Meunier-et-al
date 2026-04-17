# Plot single-country seasonal day-of-year NO2 overlays from version2 country CSVs

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(ggplot2)
    library(readr)

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- "/Users/chrispohweitiong/Documents/Claude Codex Projects/9 Isolating NO2"
        data_dir <- file.path(project_dir, "04_output", "data", "version2")
        charts_dir <- file.path(project_dir, "04_output", "charts", "version2")

    # Specific settings ---------------------------------------------------------
        target_country_iso3 <- "THA"             # SGP, MYS, IDN, THA, PHL, VNM, IND, KOR, TWN, HKG, CHN, JPN, AUS, NZL
        target_country_name <- "Thailand"
        target_country_slug <- "thailand"
        output_date_stub <- "2019-01_to_2026-04"

        # Options:
        #   "daily"  -> country_no2_index_daily
        #   "14dma"  -> country_no2_index_14dma
        #   "28dma"  -> country_no2_index_28dma
        series_to_plot <- "28dma"

        country_input_path <- file.path(
            data_dir,
            paste0(
                "no2_daily_country_",
                target_country_slug,
                "_",
                output_date_stub,
                "_version2.csv"
            )
        )

        output_path <- file.path(
            charts_dir,
            paste0(
                target_country_slug,
                "_no2_seasonal_overlay_",
                series_to_plot,
                "_",
                output_date_stub,
                "_version2.png"
            )
        )

    # Plot settings -------------------------------------------------------------
        year_levels <- as.character(2019:2026)
        year_colour_map <- c(
            "2019" = "#d9d9d9",
            "2020" = "#355070",
            "2021" = "#c6c6c6",
            "2022" = "#b8b8b8",
            "2023" = "#a8a8a8",
            "2024" = "#969696",
            "2025" = "#7f7f7f",
            "2026" = "#b23a48"
        )
        year_linewidth_map <- c(
            "2019" = 0.4,
            "2020" = 0.9,
            "2021" = 0.4,
            "2022" = 0.4,
            "2023" = 0.4,
            "2024" = 0.4,
            "2025" = 0.5,
            "2026" = 1.1
        )
        chart_width_inches <- 12
        chart_height_inches <- 6.8
        chart_dpi <- 300

# Validation -------------------------------------------------------------------
    valid_series_options <- c("daily", "14dma", "28dma")

    if (!(series_to_plot %in% valid_series_options)) {
        stop(
            "`series_to_plot` must be one of: ",
            paste(valid_series_options, collapse = ", "),
            "."
        )
    }

    if (!file.exists(country_input_path)) {
        stop(
            "Single-country version2 NO2 input not found at `",
            country_input_path,
            "`."
        )
    }

    dir.create(charts_dir, recursive = TRUE, showWarnings = FALSE)

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

    resolve_series_column <- function(series_key) {
        if (series_key == "daily") {
            return("country_no2_index_daily")
        }

        if (series_key == "14dma") {
            return("country_no2_index_14dma")
        }

        if (series_key == "28dma") {
            return("country_no2_index_28dma")
        }

        stop("Unsupported `series_to_plot`: ", series_key)
    }

    resolve_series_label <- function(series_key) {
        if (series_key == "daily") {
            return("Daily")
        }

        if (series_key == "14dma") {
            return("14-Day Moving Average")
        }

        if (series_key == "28dma") {
            return("28-Day Moving Average")
        }

        stop("Unsupported `series_to_plot`: ", series_key)
    }

# Prepare plotting data --------------------------------------------------------
    series_column_name <- resolve_series_column(series_to_plot)
    series_label <- resolve_series_label(series_to_plot)

    daily_country <- read_csv(
        country_input_path,
        show_col_types = FALSE,
        col_types = cols(
            date = col_character()
        )
    ) %>%
        mutate(
            date = as.Date(substr(date, 1, 10)),
            year = format(date, "%Y"),
            plot_date = as.Date(paste0("2020-", format(date, "%m-%d"))),
            year = factor(year, levels = year_levels)
        ) %>%
        mutate(country_no2_index_selected = .data[[series_column_name]]) %>%
        filter(!is.na(plot_date), !is.na(year))

    country_values <- unique(daily_country$country_name)
    country_iso3_values <- unique(daily_country$country_iso3)

    if (length(country_values) != 1 || country_values[[1]] != target_country_name) {
        stop(
            "The version2 seasonal plotting input should contain exactly one country: ",
            target_country_name,
            "."
        )
    }

    if (length(country_iso3_values) != 1 || country_iso3_values[[1]] != target_country_iso3) {
        stop(
            "The version2 seasonal plotting input should contain exactly one ISO3 code: ",
            target_country_iso3,
            "."
        )
    }

# Main process -----------------------------------------------------------------
    chart_object <- ggplot(
        daily_country,
        aes(
            x = plot_date,
            y = country_no2_index_selected,
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
            title = paste0(target_country_name, ": Seasonal NO2 Overlay (", series_label, ")"),
            subtitle = paste(
                "Years 2019-2025 are mostly muted, with 2020 highlighted as the COVID benchmark,",
                "and 2026 emphasized as the current-year line."
            ),
            y = "Tropospheric NO2 (mol/m^2)"
        ) +
        guides(linewidth = "none") +
        build_base_theme()

    ggsave(
        filename = output_path,
        plot = chart_object,
        width = chart_width_inches,
        height = chart_height_inches,
        dpi = chart_dpi
    )

    message("Single-country version2 seasonal overlay chart: ", output_path)
    message("Single-country version2 seasonal plotting complete.")
