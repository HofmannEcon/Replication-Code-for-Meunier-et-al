# Build single-country city list from UN WUP 2025 city population data

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(readr)
    library(readxl)

# Settings ---------------------------------------------------------------------
    # General settings
        project_dir <- "xxx"
        input_path <- file.path(project_dir, "raw_data", "WUP2025-F21-DEGURBA-Cities_Pop.xlsx")
        input_sheet <- "Data"

    # Specific settings
        target_country_iso3 <- "PHL"           # SGP, MYS, IDN, THA, PHL, VNM, IND, KOR, TWN, HKG, CHN, JPN, AUS, NZL
        target_country_name <- "Philippines"
        target_country_slug <- "philippines"
        population_year <- "2025"
        population_threshold_thousands <- 275
        max_cities <- 25

        population_only_output_path <- file.path(
            project_dir,
            "01_city_definitions",
            paste0(target_country_slug, "_cities_population_only.csv")
        )
        output_path <- file.path(
            project_dir,
            "01_city_definitions",
            paste0(target_country_slug, "_cities.csv")
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

    single_country_city_population <- wup_city_population %>%
        filter(ISO3_Code == target_country_iso3) %>%
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

# Main process -----------------------------------------------------------------
    population_only_city_list <- single_country_city_population %>%
        arrange(desc(population_thousands), city_name) %>%
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

    if (target_country_iso3 == "SGP") {
        final_city_list <- single_country_city_population %>%
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

        if (nrow(final_city_list) != 1) {
            stop("Singapore special-case row was not found in the UN city table.")
        }
    } else {
        final_city_list <- population_only_city_list %>%
            arrange(desc(population_thousands), city_name) %>%
            slice_head(n = max_cities) %>%
            left_join(
                single_country_city_population %>%
                    select(un_city_code, longitude, latitude),
                by = "un_city_code"
            ) %>%
            mutate(
                coordinate_source = "UN WUP 2025 population-weighted centroid"
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

        if (nrow(population_only_city_list) > max_cities) {
            final_city_list <- final_city_list %>%
                mutate(
                    selection_note = paste0(
                        "UN WUP 2025 city population > ",
                        population_threshold_thousands,
                        " thousand; capped to top ",
                        max_cities,
                        " cities for this country."
                    )
                )
        }
    }

    if (nrow(final_city_list) == 0) {
        stop("No ", target_country_name, " cities met the current selection rule.")
    }

# Export data ------------------------------------------------------------------
    write_csv(population_only_city_list, population_only_output_path)
    write_csv(final_city_list, output_path)

    print(final_city_list %>% select(country_iso3, country_name, city_name, population_thousands))
    message(target_country_name, " population-only city list written to: ", population_only_output_path)
    message("Number of ", target_country_name, " population-only candidate cities: ", nrow(population_only_city_list))
    message(target_country_name, " city list written to: ", output_path)
    message("Number of ", target_country_name, " cities selected: ", nrow(final_city_list))
