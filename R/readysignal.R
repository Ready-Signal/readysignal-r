#-----------#
# CONSTANTS #
#-----------#

api_base <- "https://app.readysignal.com/api"


#--------------------#
# EXPORTED FUNCTIONS #
#--------------------#

#' List Signals
#'
#' lists all the signals associated with the User access token
#'
#' @return A data.frame containing the list of signals
#' @param token User access token
#' @export
list_signals <- function(token) {
  auth <- build_auth(token)
  url <- sprintf("%s/signals", api_base)
  resp <- httr::GET(url, httr::add_headers(Authorization = auth))

  json <- jsonlite::fromJSON(
    httr::content(resp, "text", encoding = "UTF8")
  )
  data <- json$data

  return(data)
}


#' List Signal Details
#'
#' shows the details for a specific signal
#'
#' @param token User access token
#' @param signal_id Signal ID
#' @param optimized Optional parameter for optimized signal details
#' @return A data.frame containing the details for a given signal
#' @export
get_signal_details <- function(token, signal_id, optimized = NULL) {
  auth <- build_auth(token)
  url <- sprintf("%s/signals/%s", api_base, signal_id)
  
  # Build query parameters
  query_params <- list()
  if (!is.null(optimized)) {
    query_params$optimized <- optimized
  }
  
  resp <- httr::GET(
    url, 
    httr::add_headers(Authorization = auth),
    query = if (length(query_params) > 0) query_params else NULL
  )

  json <- jsonlite::fromJSON(
    httr::content(resp, "text", encoding = "UTF8")
  )
  data <- json$data

  return(data)
}


#' Get Signal
#'
#' returns a signal's data in data.frame format
#'
#' @param token User access token
#' @param signal_id Signal ID
#' @param infer_types Whether to infer column data types (defaults to TRUE)
#' @param optimized Optional parameter for optimized signal output
#' @param startDate Optional start date filter (YYYY-MM-DD format)
#' @param endDate Optional end date filter (YYYY-MM-DD format)
#' @param useTargetVariableDates Optional parameter to use target variable dates
#' @return A data.frame containing the data for a signal
#' @importFrom utils type.convert
#' @export
get_signal <- function(token, signal_id, infer_types = TRUE, optimized = NULL, startDate = NULL, endDate = NULL, useTargetVariableDates = NULL) {
  auth <- build_auth(token)
  
  # Build query parameters
  query_params <- list(page = 1)
  if (!is.null(optimized)) {
    query_params$optimized <- optimized
  }
  if (!is.null(startDate)) {
    query_params$startDate <- startDate
  }
  if (!is.null(endDate)) {
    query_params$endDate <- endDate
  }
  if (!is.null(useTargetVariableDates)) {
    query_params$useTargetVariableDates <- useTargetVariableDates
  }
  
  url <- sprintf("%s/signals/%s/output", api_base, signal_id)
  url <- httr::modify_url(url, query = query_params)
  sesh <- rvest::session(url, httr::add_headers(Authorization = auth))

  # Check HTTP status before parsing JSON
  if (sesh$response$status != 200) {
    stop(sprintf(
      "Connection to Ready Signal failed with status %d. Check that your access token is up-to-date and signal ID is valid.",
      sesh$response$status
    ))
  }

  json <- jsonlite::fromJSON(
    httr::content(sesh$response, "text", encoding = "UTF8")
  )
  data <- json$data

  ## TODO when get_signal_details contains # of rows,
  ## handle the progress bar better. that way, we could
  ## call the details first to get the number of rows, and
  ## and build the progress bar off of that, rather than
  ## making a potentially large request before making the bar

  # make the progress bar if gonna be needing to paginate
  if (json$last_page > 1) {
    pb <- progress::progress_bar$new(
      format = " [:bar] :percent / :elapsed",
      total = json$last_page - 1,
      clear = FALSE,
      width = 60
    )
  }

  while (json$current_page < json$last_page) {
    # Add page parameter to existing params (R uses copy-on-modify, equivalent to Python's .copy())
    page_params <- query_params
    page_params$page <- json$current_page + 1
    
    url <- sprintf("%s/signals/%s/output", api_base, signal_id)
    url <- httr::modify_url(url, query = page_params)
    sesh <- rvest::session_jump_to(sesh, url)

    while (sesh$response$status != 200) {
      Sys.sleep(10)
      sesh <- rvest::session_jump_to(sesh, url)
    }

    json <- jsonlite::fromJSON(
      httr::content(sesh$response, "text", encoding = "UTF8")
    )
    data <- rbind(data, json$data)

    pb$tick()
  }

  names(data) <- gsub("-", "_", names(data))

  if (infer_types) {
    for (i in seq_len(ncol(data))) {
      tryCatch(
        {
          data[[i]] <- as.Date(data[[i]])
          next
        },
        warning = function(e) {},
        error = function(e) {}
      )

      tryCatch(
        {
          data[[i]] <- type.convert(data[[i]])
        },
        warning = function(e) {},
        error = function(e) {}
      )
    }
  }

  return(data)
}


#' Save Signal to CSV
#'
#' saves signal data to CSV file
#'
#' @param token User access token
#' @param signal_id Signal ID
#' @param file_name File name for the CSV
#' @importFrom utils write.csv
#' @return None, function writes data to file system
#' @export
signal_to_csv <- function(token, signal_id, file_name) {
  auth <- build_auth(token)
  df <- get_signal(token, signal_id)
  write.csv(df, file_name)
}


#' Auto Discover
#'
#' create a signal with the Auto Discover feature
#'
#' @param token User access token
#' @param geo_grain Geo grain of upload, "State" or "Country"
#' @param date_grain Date grain of upload, "Day" or "Month"
#' @param filename Filename of .CSV or .XLS with "Date", "Value", "State"
#' (if geo_grain=State) columns. Not to be used with `df`
#' @param df DataFrame with "Date", "Value", "State" (if geo_grain=State).
#' Not to be used with `filename`
#' @param create_custom_features Flag for custom feature creation. 0 or 1, defaults to 1
#' @param callback_url Callback URL for notifications
#' @param filtered_geo_grains Optional parameter to filter geographic grains: "usa", "nonusa", or "all"
#' @param signal_name Optional parameter to specify a custom name for the signal (max 255 characters).
#' If not provided, the signal name will default to 'Auto-discovery - [custom feature name]'
#' @return HTTP response
#' @export
auto_discover <- function(token, geo_grain, date_grain, filename = NULL, df = NULL, create_custom_features = 1, callback_url = NULL, filtered_geo_grains = NULL, signal_name = NULL) {
  auth <- build_auth(token)

  if (!geo_grain %in% c("State", "Country")) {
    stop("`geo_grain` must be \"State\" or \"Country\"")
  }

  if (!date_grain %in% c("Day", "Month")) {
    stop("`date_grain` must by \"Day\" or \"Month\"")
  }

  if (!create_custom_features %in% c(0, 1)) {
    stop("`create_custom_features` must by 0 or 1")
  }
  
  if (!is.null(filtered_geo_grains) && !filtered_geo_grains %in% c("usa", "nonusa", "all")) {
    stop("`filtered_geo_grains` must be \"usa\", \"nonusa\", or \"all\"")
  }

  if (is.null(callback_url)) {
    callback_url <- ""
  }

  if (!is.null(filename)) {
    url <- sprintf("%s/auto-discovery/file", api_base)
    body_list <- list(
      callback_url = callback_url,
      create_custom_features = create_custom_features,
      geo_grain = geo_grain,
      date_grain = date_grain,
      file = httr::upload_file(filename)
    )
    if (!is.null(filtered_geo_grains)) {
      body_list$filtered_geo_grains <- filtered_geo_grains
    }
    if (!is.null(signal_name)) {
      body_list$signal_name <- signal_name
    }
    resp <- httr::POST(
      url,
      body = body_list,
      httr::add_headers(Authorization = auth)
    )
  } else if (!is.null(df)) {
    url <- sprintf("%s/auto-discovery/array", api_base)
    body <- list(
      callback_url = callback_url,
      create_custom_features = create_custom_features,
      geo_grain = geo_grain,
      date_grain = date_grain,
      data = df
    )
    if (!is.null(filtered_geo_grains)) {
      body$filtered_geo_grains <- filtered_geo_grains
    }
    if (!is.null(signal_name)) {
      body$signal_name <- signal_name
    }
    body <- jsonlite::toJSON(body, auto_unbox = TRUE)
    body <- as.character(body)    
    resp <- httr::POST(
      url,
      body = body,
      httr::add_headers(
        Authorization = auth,
        "Content-Type" = "application/json"
      )
    )
  } else {
    stop("Missing data source, please provide `filename` or `df`")
  }

  return(httr::content(resp))
}


#' Delete Signal
#'
#' deletes a signal
#'
#' @param token User access token
#' @param signal_id Signal ID
#' @return HTTP response
#' @export
delete_signal <- function(token, signal_id) {
  auth <- build_auth(token)
  url <- sprintf("%s/signals/%s", api_base, signal_id)
  resp <- httr::DELETE(url, httr::add_headers(Authorization = auth))
  return(httr::content(resp))
}


#--------------------#
# INTERNAL FUNCTIONS #
#--------------------#

build_auth <- function(token) {
  #
  # build and check auth header, raise
  # an error if unable to list signals
  #
  # TODO this could use work. a better
  # way to catch token errors without
  # the extra overhead
  #
  auth <- paste0("Bearer ", token)
  resp <- httr::GET(
    sprintf("%s/signals", api_base),
    httr::add_headers(Authorization = auth)
  )
  if (resp$status_code == 401) {
    stop("Failed to authenticate, please confirm your access token is correct.")
  } else {
    return(auth)
  }
}
