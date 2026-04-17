# Build combined Asia-Pacific city list from UN WUP 2025 city population data

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(readr)
    library(readxl)

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- "/Users/chrispohweitiong/Documents/Claude Codex Projects/9 Isolating NO2"
        input_path <- file.path(project_dir, "raw_data", "WUP2025-F21-DEGURBA-Cities_Pop.xlsx")
        population_only_output_path <- file.path(
            project_dir,
            "01_city_definitions",
            "apac_cities_population_only.csv"
        )
        output_path <- file.path(
            project_dir,
            "01_city_definitions",
            "apac_cities.csv"
        )
        input_sheet <- "Data"

    # Specific settings ---------------------------------------------------------
        population_year <- "2025"
        population_threshold_thousands <- 275
        max_cities_per_country <- 25
        target_iso3 <- c(
            "SGP", "MYS", "IDN", "THA", "PHL", "VNM",
            "IND",
            "CHN", "JPN", "KOR", "TWN", "HKG",
            "AUS", "NZL"
        )
        single_city_overrides <- tibble::tibble(
            country_iso3 = "SGP",
            city_name = "Singapore",
            selection_note_override = "Special case: use one Singapore observation only."
        )

# Validation -------------------------------------------------------------------
    if (!file.exists(input_path)) {
        stop("UN WUP city population file not found in `raw_data/`.")
    }

# Prepare raw data -------------------------------------------------------------
    wup_city_population <- read_excel(input_path, sheet = input_sheet)

    if (!population_year %in% names(wup_city_population)) {
        stop("Population year column `", population_year, "` was not found in the UN file.")
    }

    regional_city_population <- wup_city_population %>%
        filter(ISO3_Code %in% target_iso3) %>%
        transmute(
            country_iso3 = ISO3_Code,
            country_name = Location,
            un_city_code = City_Code,
            city_name = City_Name,
            longitude = PWCent_Longitude,
            latitude = PWCent_Latitude,
            population_thousands = .data[[population_year]]
        ) %>%
        filter(
            !is.na(population_thousands),
            population_thousands > population_threshold_thousands
        )

    special_countries <- unique(single_city_overrides$country_iso3)

# Main process -----------------------------------------------------------------
    population_only_city_list <- regional_city_population %>%
        arrange(country_iso3, desc(population_thousands), city_name) %>%
        mutate(
            population_year = as.integer(population_year),
            population_unit = "thousands",
            selection_note = paste0(
                "UN WUP 2025 city population > ",
                population_threshold_thousands,
                " thousand."
            ),
            source_file = basename(input_path)
        ) %>%
        select(
            country_iso3,
            country_name,
            un_city_code,
            city_name,
            population_year,
            population_thousands,
            population_unit,
            selection_note,
            source_file
        )

    special_city_list <- regional_city_population %>%
        inner_join(
            single_city_overrides,
            by = c("country_iso3", "city_name")
        ) %>%
        mutate(
            population_year = as.integer(population_year),
            population_unit = "thousands",
            coordinate_source = "UN WUP 2025 population-weighted centroid",
            selection_note = selection_note_override,
            source_file = basename(input_path)
        ) %>%
        select(
            country_iso3,
            country_name,
            un_city_code,
            city_name,
            longitude,
            latitude,
            population_year,
            population_thousands,
            population_unit,
            coordinate_source,
            selection_note,
            source_file
        )

    expected_special_rows <- nrow(single_city_overrides)

    if (nrow(special_city_list) != expected_special_rows) {
        stop("One or more single-city override rows were not found in the UN city table.")
    }

    capped_economies <- population_only_city_list %>%
        filter(!country_iso3 %in% special_countries) %>%
        count(country_iso3, name = "n_candidates") %>%
        filter(n_candidates > max_cities_per_country) %>%
        pull(country_iso3)

    regular_final_city_list <- population_only_city_list %>%
        filter(!country_iso3 %in% special_countries) %>%
        arrange(country_iso3, desc(population_thousands), city_name) %>%
        group_by(country_iso3) %>%
        slice_head(n = max_cities_per_country) %>%
        ungroup() %>%
        left_join(
            regional_city_population %>%
                select(un_city_code, longitude, latitude),
            by = "un_city_code"
        ) %>%
        mutate(
            coordinate_source = "UN WUP 2025 population-weighted centroid",
            selection_note = if_else(
                country_iso3 %in% capped_economies,
                paste0(
                    "UN WUP 2025 city population > ",
                    population_threshold_thousands,
                    " thousand; capped to top ",
                    max_cities_per_country,
                    " cities for this economy."
                ),
                selection_note
            )
        ) %>%
        select(
            country_iso3,
            country_name,
            un_city_code,
            city_name,
            longitude,
            latitude,
            population_year,
            population_thousands,
            population_unit,
            coordinate_source,
            selection_note,
            source_file
        )

    final_city_list <- bind_rows(
        special_city_list,
        regular_final_city_list
    ) %>%
        arrange(country_iso3, desc(population_thousands), city_name)

    if (nrow(final_city_list) == 0) {
        stop("No combined Asia-Pacific cities met the current selection rule.")
    }

# Export data ------------------------------------------------------------------
    write_csv(population_only_city_list, population_only_output_path)
    write_csv(final_city_list, output_path)

    print(final_city_list %>% count(country_iso3, country_name, name = "n_cities"))
    message("APAC population-only city list written to: ", population_only_output_path)
    message("Number of APAC population-only candidate cities: ", nrow(population_only_city_list))
    message("APAC city list written to: ", output_path)
    message("Number of APAC cities selected: ", nrow(final_city_list))
