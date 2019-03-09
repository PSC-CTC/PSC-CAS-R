################
#
# Helper functions to extract data from a CAS model database
#
# Nicholas Komick
# nicholas.komick@dfo-mpo.gc.ca
# Feb 23, 2017
# Coding Style: http://google-styleguide.googlecode.com/svn/trunk/google-r-style.html
#
################

CASRecTblNames <- c("CWDBRecovery", "ERA_CWDBRecovery")
CASFisheryTblNames <- c("Fishery", "ERA_Fishery")

GetBaseRecoveriesSqlFilename <- "./sql/GetBaseRecoveries.sql"


TranslateDbColumnNames <- function(data) {
  names(data)<- gsub("_", ".", names(data)) 
  return (data)
}


#' Retreives Sql Statement text, binds parameters to the text and basic format cleanup
#'
#' @param sql_file_name The name of of the file with sql to load
#' @param params List of named parameters to bind to the sql text
#' @param sql_dir The directory that stores all the sql statements (defaults to "sql")
#' @param clean_sql A boolean that identifies if the format of the final sql should be cleaned (e.g. strip newlines)
#' @param package_name The package name to load sql file from, defaults to the current package name
#'
#' @return An Sql text statement
#'
#' @importFrom stringr str_interp str_replace_all str_glue
#' @importFrom futile.logger flog.error
#'
formatSql <- function(sql_file_name, params = NULL, clean_sql = TRUE) {
  sql_file_path <- normalizePath(sql_file_name)
  if (file.exists(sql_file_path) == FALSE) {
    error_msg <- str_interp("ERROR - The sql file '${sql_file_name}' is missing.")
    stop(error_msg)
  }
  sql_text <- readChar(sql_file_path, file.info(sql_file_path)$size)
  
  if (!is.null(params)) {
    sql_text <- str_interp(sql_text, params)
  }
  
  if (clean_sql) {
    sql_text <- str_replace_all(sql_text, "\r\n", " ")
  }
  return(sql_text)
}

#' Generic function that retrieves data from database using sql file
#'
#' @param db_conn A connection to the MRP Operational database
#' @param sql_filename A text file containing SQL file
#' @param params Parameters to insert into the SQL statement
#'
#' @importFrom dplyr %>% as_tibble
#' @importFrom DBI dbGetQuery
#' @importFrom stringr str_to_lower
#'
getDbData <- function(db_conn, sql_filename, params = NULL) {
  sql_text <- formatSql(sql_filename, params)
  
  data_result <-
    sqlQuery(db_conn, sql_text)

  data_result <- TranslateDbColumnNames(data_result)
  return(data_result)
}


#' Get the base recoveries from the database, the function tries to
#' retreives either the CAS or CIS version of recoveries
#'
#' @param db_conn A connection to the MRP Operational database
#'
getBaseRecoveries <- function (db_conn){
  #Find the approriate table name
  table_names <- sqlTables(db_conn)$TABLE_NAME
  rec_tbl_name <- CASRecTblNames[CASRecTblNames %in% table_names]
  fishery_tbl_name <- CASFisheryTblNames[CASFisheryTblNames %in% table_names]
  data <- getDbData(db_conn, 
                    GetBaseRecoveriesSqlFilename, 
                    list(rec_tbl_name = rec_tbl_name,
                         fishery_tbl_name = fishery_tbl_name))
  return (data)
}