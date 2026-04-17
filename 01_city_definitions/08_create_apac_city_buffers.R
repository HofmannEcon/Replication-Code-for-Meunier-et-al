# Create 40 km city buffers from APAC city definitions

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(jsonlite)
    library(readr)
    library(sf)

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- "xxx"
        input_path <- file.path(project_dir, "01_city_definitions", "apac_cities.csv")
        output_path <- file.path(
            project_dir,
            "01_city_definitions",
            "apac_cities_with_buffers.csv"
        )

    # Specific settings ---------------------------------------------------------
        buffer_radius_m <- 40000
        coordinate_crs <- 4326
        buffer_method <- "sf::st_buffer with s2 geodesic buffering"
        geojson_digits <- 7

# Validation -------------------------------------------------------------------
    if (!file.exists(input_path)) {
        stop("City definitions file not found at `01_city_definitions/apac_cities.csv`.")
    }

# Prepare raw data -------------------------------------------------------------
    city_definitions <- read_csv(input_path, show_col_types = FALSE)

    required_columns <- c("country_iso3", "country_name", "city_name", "longitude", "latitude")
    missing_columns <- setdiff(required_columns, names(city_definitions))

    if (length(missing_columns) > 0) {
        stop("Missing required columns: ", paste(missing_columns, collapse = ", "))
    }

    if (any(is.na(city_definitions$longitude)) || any(is.na(city_definitions$latitude))) {
        stop("Longitude and latitude must be present for every city.")
    }

    if (any(city_definitions$longitude < -180 | city_definitions$longitude > 180)) {
        stop("Longitude values must fall between -180 and 180.")
    }

    if (any(city_definitions$latitude < -90 | city_definitions$latitude > 90)) {
        stop("Latitude values must fall between -90 and 90.")
    }

# Helper functions -------------------------------------------------------------
    coordinate_matrix_to_pairs <- function(coord_matrix) {
        lapply(
            seq_len(nrow(coord_matrix)),
            function(row_index) {
                unname(as.numeric(coord_matrix[row_index, c("X", "Y"), drop = TRUE]))
            }
        )
    }

    geometry_to_geojson <- function(geometry) {
        geometry_type <- as.character(st_geometry_type(geometry))
        coord_matrix <- st_coordinates(geometry)

        if (geometry_type == "POLYGON") {
            ring_ids <- unique(coord_matrix[, "L1"])
            polygon_coordinates <- lapply(
                ring_ids,
                function(ring_id) {
                    ring_matrix <- coord_matrix[coord_matrix[, "L1"] == ring_id, , drop = FALSE]
                    coordinate_matrix_to_pairs(ring_matrix)
                }
            )

            geojson_object <- list(
                type = "Polygon",
                coordinates = polygon_coordinates
            )
        } else if (geometry_type == "MULTIPOLYGON") {
            polygon_ids <- unique(coord_matrix[, "L2"])
            multipolygon_coordinates <- lapply(
                polygon_ids,
                function(polygon_id) {
                    polygon_matrix <- coord_matrix[coord_matrix[, "L2"] == polygon_id, , drop = FALSE]
                    ring_ids <- unique(polygon_matrix[, "L1"])

                    lapply(
                        ring_ids,
                        function(ring_id) {
                            ring_matrix <- polygon_matrix[polygon_matrix[, "L1"] == ring_id, , drop = FALSE]
                            coordinate_matrix_to_pairs(ring_matrix)
                        }
                    )
                }
            )

            geojson_object <- list(
                type = "MultiPolygon",
                coordinates = multipolygon_coordinates
            )
        } else {
            stop("Unsupported geometry type for GeoJSON export: ", geometry_type)
        }

        toJSON(
            geojson_object,
            auto_unbox = TRUE,
            digits = geojson_digits,
            null = "null"
        )
    }

# Main process -----------------------------------------------------------------
    sf_use_s2(TRUE)

    city_points <- st_as_sf(
        city_definitions,
        coords = c("longitude", "latitude"),
        crs = coordinate_crs,
        remove = FALSE
    )

    city_buffers <- st_buffer(city_points, dist = buffer_radius_m)

    invalid_geometry_count <- sum(!st_is_valid(city_buffers))

    if (invalid_geometry_count > 0) {
        city_buffers <- st_make_valid(city_buffers)
    }

    city_buffers_with_geojson <- city_buffers %>%
        mutate(
            geometry_geojson = vapply(
                st_geometry(.),
                FUN = geometry_to_geojson,
                FUN.VALUE = character(1)
            ),
            buffer_radius_m = buffer_radius_m,
            buffer_method = buffer_method,
            geometry_crs = "EPSG:4326"
        ) %>%
        st_drop_geometry()

    if (nrow(city_buffers_with_geojson) != nrow(city_definitions)) {
        stop("Output row count does not match the input city definitions.")
    }

    if (any(is.na(city_buffers_with_geojson$geometry_geojson)) ||
        any(city_buffers_with_geojson$geometry_geojson == "")) {
        stop("One or more cities are missing GeoJSON buffer output.")
    }

# Export data ------------------------------------------------------------------
    write_csv(city_buffers_with_geojson, output_path)

    message("APAC city buffers written to: ", output_path)
    message("Number of APAC city buffers created: ", nrow(city_buffers_with_geojson))
    message("Invalid geometries repaired during run: ", invalid_geometry_count)
