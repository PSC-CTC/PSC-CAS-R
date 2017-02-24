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

kCasBaseRecoveries <- "./sql/GetCasBaseRecoveries.sql"


TranslateDbColumnNames <- function(data) {
  names(data)<- gsub("_", ".", names(data)) 
  return (data)
}

RunSqlFile <- function (db.conn, file.name, variables=NA) {
  # A helper function that loads an SQL script, updates the variables in the script to values provide and
  # formats the resulting data by renames columns to common R style.
  #
  # Args:
  #   db.conn: An odbc connection to the ODBC database
  #   file.name: A file name that the SQL script is saved to
  #   variables: An R list of variables, variable names in the list are matched to ones with the same name in
  #       a format like %VARIABLENAME% (eg list(runid = 1) will replace %RUNID% in the SQL with 1)
  #
  # Returns:
  #   A data frame with query results
  #
  # Exceptions:
  #   If a variable type is found that the function can't handle (e.g. a vector), the script
  #   will throw an exception.
  #     
  file.conn <- file(file.name, "r", blocking = FALSE)
  sql.text <- paste(readLines(file.conn), collapse=" ")# empty
  close(file.conn)
  
  if (is.list(variables)) {
    var.names <- names(variables)
    
    for (var.idx in 1:length(var.names)) {
      var.name <- var.names[var.idx]
      var.value <- variables[[var.name]]
      if (is.numeric(var.value)) {
        sql.text <- gsub(paste0("%", var.name, "%"), var.value, sql.text, ignore.case=TRUE)
      } else if (is.character(var.value) || is.factor(var.value)) {
        sql.text <- gsub(paste0("%", var.name, "%"), 
                         paste0("'", as.character(var.value), "'"), 
                         sql.text, 
                         ignore.case=TRUE)
      } else {
        stop(sprintf("Unknown variable type '%s' for variable '%s' when converting in RunSqlFile", typeof(var.value), var.name))
      }
    }
  }
  
  unbound.variables <- gregexpr("%[a-z]*%", sql.text, ignore.case=TRUE)
  if (unbound.variables[[1]] > 0) {
    error.msg <- sprintf("Unbound variables found for the '%s' sql script \n", file.name)
    stop(error.msg)
  }
  
  data <- sqlQuery(db.conn, sql.text)
  data <- TranslateDbColumnNames(data)
  return (data)   
}

GetCasBaseRecoveries <- function (cas.db.conn){
  data <- RunSqlFile(cas.db.conn, kCasBaseRecoveries)
  return (data)
}