# Time Series Cross Validation for IP nowcasting benchmarks

# Load packages ----------------------------------------------------------------
    library(dplyr)
    library(ggplot2)
    library(purrr)
    library(readr)
    library(rsample)
    library(tibble)
    library(zoo)

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- normalizePath(".", winslash = "/", mustWork = TRUE)
        input_path <- file.path(project_dir, "05_regression", "SampleReg.csv")
        num_origins <- 24
        forecast_horizon <- 1

        splits_output_path <- file.path(project_dir, "05_regression", "tscv_splits_summary.csv")
        forecasts_output_path <- file.path(project_dir, "05_regression", "tscv_forecasts_by_origin.csv")
        mase_output_path <- file.path(project_dir, "05_regression", "tscv_mase_by_origin.csv")
        boxplot_output_csv_path <- file.path(project_dir, "05_regression", "tscv_mase_boxplot_data.csv")
        boxplot_output_png_path <- file.path(project_dir, "05_regression", "tscv_mase_boxplot.png")

    # Variable settings ---------------------------------------------------------
        date_column <- "Date"
        ip_column <- "IP"
        pmi_column <- "PMI"
        no2_column <- "NO2"

# Validation -------------------------------------------------------------------
    if (!file.exists(input_path)) {
        stop("Regression input file not found at `05_regression/SampleReg.csv`.")
    }

# Prepare raw data -------------------------------------------------------------
    raw <- read_csv(input_path, show_col_types = FALSE)

    if (!all(c(date_column, ip_column, pmi_column, no2_column) %in% names(raw))) {
        stop("The regression input file is missing one or more required columns.")
    }

    raw_clean <- raw %>%
        filter(.data[[date_column]] != "Unit") %>%
        transmute(
            Date = as.Date(as.yearmon(.data[[date_column]], format = "%b-%y")),
            IP = as.numeric(.data[[ip_column]]),
            PMI = as.numeric(.data[[pmi_column]]),
            NO2 = as.numeric(.data[[no2_column]])
        ) %>%
        arrange(Date)

# Transform variables ----------------------------------------------------------
    regression_df <- raw_clean %>%
        mutate(
            IP_yoy = 100 * (IP / lag(IP, 12) - 1),
            NO2_yoy = 100 * (NO2 / lag(NO2, 12) - 1)
        ) %>%
        mutate(
            IP_yoy_lag1 = lag(IP_yoy, 1)
        ) %>%
        transmute(
            Date = Date,
            IP_yoy = IP_yoy,
            IP_yoy_lag1 = IP_yoy_lag1,
            PMI = PMI,
            NO2_yoy = NO2_yoy
        ) %>%
        filter(complete.cases(.))

    if (nrow(regression_df) == 0) {
        stop("No complete observations are available after the IP and NO2 year-on-year transformations.")
    }

# Establish rolling origin -----------------------------------------------------
    total_observations <- nrow(regression_df)
    initial_length <- total_observations - ((num_origins - 1) + forecast_horizon)

    if (initial_length <= 0) {
        stop("Not enough complete observations for the chosen number of origins and forecast horizon.")
    }

    splits <- rolling_origin(
        regression_df,
        initial = initial_length,
        assess = forecast_horizon,
        cumulative = TRUE,
        skip = 0
    )

    splits$splits <- splits$splits[seq_len(num_origins)]
    splits$id <- splits$id[seq_len(num_origins)]

    splits_summary <- map_dfr(seq_along(splits$splits), function(i) {
        train_df <- analysis(splits$splits[[i]])
        test_df <- assessment(splits$splits[[i]])

        tibble(
            origin = i,
            train_n = nrow(train_df),
            test_n = nrow(test_df),
            train_start = min(train_df$Date),
            train_end = max(train_df$Date),
            test_start = min(test_df$Date),
            test_end = max(test_df$Date)
        )
    })

# Run benchmark models ---------------------------------------------------------
    forecast_results <- map_dfr(seq_along(splits$splits), function(i) {
        train_df <- analysis(splits$splits[[i]])
        test_df <- assessment(splits$splits[[i]])
        naive_scale <- mean(abs(diff(train_df$IP_yoy)), na.rm = TRUE)

        if (is.na(naive_scale) || naive_scale <= 0) {
            stop("The naive scaling term for MASE is missing or non-positive in one of the training windows.")
        }

        ar1_fit <- lm(IP_yoy ~ IP_yoy_lag1, data = train_df)
        pmi_fit <- lm(IP_yoy ~ PMI, data = train_df)
        no2_fit <- lm(IP_yoy ~ NO2_yoy, data = train_df)
        ar1_no2_fit <- lm(IP_yoy ~ IP_yoy_lag1 + NO2_yoy, data = train_df)
        pmi_no2_fit <- lm(IP_yoy ~ PMI + NO2_yoy, data = train_df)
        ar1_pmi_no2_fit <- lm(IP_yoy ~ IP_yoy_lag1 + PMI + NO2_yoy, data = train_df)

        tibble(
            origin = i,
            Date = test_df$Date,
            actual = test_df$IP_yoy,
            ar1_pred = as.numeric(predict(ar1_fit, newdata = test_df)),
            pmi_pred = as.numeric(predict(pmi_fit, newdata = test_df)),
            no2_pred = as.numeric(predict(no2_fit, newdata = test_df)),
            ar1_no2_pred = as.numeric(predict(ar1_no2_fit, newdata = test_df)),
            pmi_no2_pred = as.numeric(predict(pmi_no2_fit, newdata = test_df)),
            ar1_pmi_no2_pred = as.numeric(predict(ar1_pmi_no2_fit, newdata = test_df)),
            naive_scale = naive_scale
        )
    })

# Build MASE table -------------------------------------------------------------
    mase_by_origin <- forecast_results %>%
        transmute(
            origin = as.character(origin),
            Date = as.character(Date),
            AR1_MASE = abs(ar1_pred - actual) / naive_scale,
            PMI_MASE = abs(pmi_pred - actual) / naive_scale,
            NO2_MASE = abs(no2_pred - actual) / naive_scale,
            AR1_NO2_MASE = abs(ar1_no2_pred - actual) / naive_scale,
            PMI_NO2_MASE = abs(pmi_no2_pred - actual) / naive_scale,
            AR1_PMI_NO2_MASE = abs(ar1_pmi_no2_pred - actual) / naive_scale
        )

    mase_average <- mase_by_origin %>%
        summarise(
            origin = "AVERAGE",
            Date = NA_character_,
            AR1_MASE = mean(AR1_MASE, na.rm = TRUE),
            PMI_MASE = mean(PMI_MASE, na.rm = TRUE),
            NO2_MASE = mean(NO2_MASE, na.rm = TRUE),
            AR1_NO2_MASE = mean(AR1_NO2_MASE, na.rm = TRUE),
            PMI_NO2_MASE = mean(PMI_NO2_MASE, na.rm = TRUE),
            AR1_PMI_NO2_MASE = mean(AR1_PMI_NO2_MASE, na.rm = TRUE)
        )

    mase_output <- bind_rows(mase_by_origin, mase_average)

    boxplot_data <- bind_rows(
        mase_by_origin %>%
            transmute(origin = origin, Date = Date, model = "AR(1)", mase = AR1_MASE),
        mase_by_origin %>%
            transmute(origin = origin, Date = Date, model = "PMI", mase = PMI_MASE),
        mase_by_origin %>%
            transmute(origin = origin, Date = Date, model = "NO2", mase = NO2_MASE),
        mase_by_origin %>%
            transmute(origin = origin, Date = Date, model = "AR(1) + NO2", mase = AR1_NO2_MASE),
        mase_by_origin %>%
            transmute(origin = origin, Date = Date, model = "PMI + NO2", mase = PMI_NO2_MASE),
        mase_by_origin %>%
            transmute(origin = origin, Date = Date, model = "AR(1) + PMI + NO2", mase = AR1_PMI_NO2_MASE)
    ) %>%
        mutate(
            model = factor(
                model,
                levels = c("AR(1)", "PMI", "NO2", "AR(1) + NO2", "PMI + NO2", "AR(1) + PMI + NO2")
            )
        ) %>%
        arrange(model, origin)

    mase_boxplot <- ggplot(boxplot_data, aes(x = model, y = mase, fill = model)) +
        geom_boxplot(width = 0.65, alpha = 0.8, outlier.shape = 21, outlier.size = 2) +
        scale_fill_manual(
            values = c(
                "AR(1)" = "#355070",
                "PMI" = "#6d597a",
                "NO2" = "#b56576",
                "AR(1) + NO2" = "#e56b6f",
                "PMI + NO2" = "#eaac8b",
                "AR(1) + PMI + NO2" = "#9c6644"
            )
        ) +
        labs(
            title = "TSCV MASE Distribution by Model",
            subtitle = paste0("12 rolling origins, one-step-ahead forecasts"),
            x = NULL,
            y = "MASE"
        ) +
        theme_minimal(base_size = 12) +
        theme(
            legend.position = "none",
            axis.text.x = element_text(angle = 15, hjust = 1),
            plot.title = element_text(face = "bold")
        )

# Export outputs ---------------------------------------------------------------
    write_csv(splits_summary, splits_output_path)
    write_csv(forecast_results, forecasts_output_path)
    write_csv(mase_output, mase_output_path)
    write_csv(boxplot_data, boxplot_output_csv_path)
    ggsave(
        filename = boxplot_output_png_path,
        plot = mase_boxplot,
        width = 9,
        height = 5.5,
        dpi = 300
    )

    message("Regression TSCV complete.")
    message("Complete observations used: ", total_observations)
    message("Origins evaluated: ", num_origins)
    message("Input file: ", input_path)
    message("Splits summary: ", splits_output_path)
    message("Forecasts by origin: ", forecasts_output_path)
    message("MASE by origin: ", mase_output_path)
    message("MASE boxplot CSV: ", boxplot_output_csv_path)
    message("MASE boxplot PNG: ", boxplot_output_png_path)
