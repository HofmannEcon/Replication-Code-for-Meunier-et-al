# CDSE authentication helper functions

# Helper functions -------------------------------------------------------------
    read_dotenv_file <- function(env_path) {
        env_lines <- readLines(env_path, warn = FALSE)
        env_lines <- trimws(env_lines)
        env_lines <- env_lines[env_lines != ""]
        env_lines <- env_lines[substr(env_lines, 1, 1) != "#"]

        if (length(env_lines) == 0) {
            return(list())
        }

        env_values <- list()

        for (line in env_lines) {
            equals_position <- regexpr("=", line, fixed = TRUE)[1]

            if (equals_position <= 1) {
                next
            }

            key <- trimws(substr(line, 1, equals_position - 1))
            value <- trimws(substr(line, equals_position + 1, nchar(line)))
            value <- sub('^"(.*)"$', "\\1", value)
            value <- sub("^'(.*)'$", "\\1", value)

            env_values[[key]] <- value
        }

        env_values
    }

    load_cdse_credentials <- function(env_path) {
        env_values <- read_dotenv_file(env_path)

        username <- env_values[["CDSE_USERNAME"]]
        password <- env_values[["CDSE_PASSWORD"]]

        if (is.null(username) || identical(username, "")) {
            stop("`CDSE_USERNAME` is missing or blank in `.env`.")
        }

        if (is.null(password) || identical(password, "")) {
            stop("`CDSE_PASSWORD` is missing or blank in `.env`.")
        }

        list(
            username = username,
            password = password
        )
    }

    request_cdse_token <- function(username,
                                   password,
                                   token_url,
                                   client_id = "cdse-public") {
        token_response <- httr2::request(token_url) |>
            httr2::req_body_form(
                client_id = client_id,
                grant_type = "password",
                username = username,
                password = password
            ) |>
            httr2::req_perform()

        token_body <- httr2::resp_body_json(token_response, simplifyVector = TRUE)

        if (is.null(token_body$access_token) || identical(token_body$access_token, "")) {
            stop("CDSE token response did not include an access token.")
        }

        token_body
    }

    get_cdse_access_token <- function(env_path,
                                      token_url,
                                      client_id = "cdse-public") {
        credentials <- load_cdse_credentials(env_path)

        request_cdse_token(
            username = credentials$username,
            password = credentials$password,
            token_url = token_url,
            client_id = client_id
        )
    }
