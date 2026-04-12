# Test CDSE authentication using credentials stored in .env

# For Windows
# A few practical notes:
# The filename should be exactly .env
# It should live in the top-level project folder
# Do not save it as .env.txt
# Quotes are optional unless you need them
# The parser in this project reads simple KEY=VALUE lines, plus comments starting with #
# On Windows, the easiest ways to create it are:
#   
#   In VS Code or Notepad, create a new file named .env in the project folder.
# Paste the two lines above and replace with your real credentials.
# Save it as “All Files”, not as a .txt file.

# Load packages ----------------------------------------------------------------
    library(httr2)

# Settings ---------------------------------------------------------------------
    # General settings ----------------------------------------------------------
        project_dir <- normalizePath(".", winslash = "/", mustWork = TRUE)
        env_path <- file.path(project_dir, ".env")
        helper_path <- file.path(project_dir, "00_setup", "cdse_auth_helpers.R")
        token_url <- paste0(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/",
            "protocol/openid-connect/token"
        )

    # Specific settings ---------------------------------------------------------
        client_id <- "cdse-public"

# Validation -------------------------------------------------------------------
    if (!file.exists(env_path)) {
        stop("`.env` was not found in the project root.")
    }

    if (!file.exists(helper_path)) {
        stop("Authentication helper file was not found at `00_setup/cdse_auth_helpers.R`.")
    }

# Helper functions -------------------------------------------------------------
    source(helper_path)

# Main process -----------------------------------------------------------------
    token_body <- get_cdse_access_token(
        env_path = env_path,
        token_url = token_url,
        client_id = client_id
    )

    expires_in_seconds <- token_body$expires_in
    token_expiry_time <- Sys.time() + as.numeric(expires_in_seconds)

    message("CDSE authentication succeeded.")
    message("Token expires in approximately ", expires_in_seconds, " seconds.")
    message("Approximate token expiry time: ", format(token_expiry_time, tz = "", usetz = TRUE))
