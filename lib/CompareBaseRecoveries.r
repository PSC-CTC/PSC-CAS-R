################
#
# A utility to compare base recoveries in the CWDBRecovery table of two different CAS databases
#
# Nicholas Komick
# nicholas.komick@dfo-mpo.gc.ca
# February 23, 2017
# Using: http://google-styleguide.googlecode.com/svn/trunk/google-r-style.html
#
################


rm(list=ls()) #clean up the workspace
options(stringsAsFactors = FALSE)
header <- "PSC CAS Base Recovery Compare v0.3"

kKeyFields <- c("RecoveryId", "Agency", "RunYear")
kPassThroughFields <- c("Auxiliary", "RecoveryMonth", "RecoverySite", "RecTagCode")
kCompareFields <- c("EstimatedNumber", "TagCode", "FisheryName")
kUsedFields <- c(kKeyFields, kPassThroughFields, kCompareFields)

source.lib.dir <- "./lib/"
if (exists("lib.dir")) {
  source.lib.dir <- lib.dir
} 

if (exists("report.dir") == FALSE) {
  report.dir <- "./report/"
}

dir.create(report.dir, showWarnings = FALSE)

util.lib.name <- file.path(source.lib.dir, "Util.r")
source(util.lib.name)

cas.db.lib.name <- file.path(source.lib.dir, "CasDbLib.r")
source(cas.db.lib.name)

required.packages <- c("RODBC", "dplyr", "parallel", "stringr", "tools", "lubridate")
InstallRequiredPackages(required.packages)

SetupCluster <- function() {
  cl <- makeCluster(2)
  clusterEvalQ(cl, library(RODBC))
  clusterEvalQ(cl, library(stringr))
  clusterEvalQ(cl, library(tools))
  clusterEvalQ(cl, library(dplyr))
  clusterExport(cl, c("cas.db.lib.name", "util.lib.name"))
  clusterEvalQ(cl, source(util.lib.name))
  clusterEvalQ(cl, source(cas.db.lib.name))
  clusterEvalQ(cl, options(stringsAsFactors = FALSE))
  return(cl)
}

db_file_filter <- rbind(Filters["All",], c("Access Database (*.mdb, *.accdb)", "*.mdb;*.accdb"))

cat(str_c(header, "\n"))
first.db.name <- choose.files(caption = "Select First CAS Database file", multi=FALSE, filters = db_file_filter)

if (length(first.db.name) == 0) {
  stop("Selecting first database was cancelled by the user.")
} else {
  cat(sprintf("First database file name: %s\n", first.db.name))
}

second.db.name <- choose.files(caption = "Select Second CAS Database file", multi=FALSE, filters = db_file_filter)

if (length(second.db.name) == 0) {
  stop("Selecting second database was cancelled by the user.")
} else {
  cat(sprintf("Second database file name: %s\n", second.db.name))
}

cl <- SetupCluster()

db.names <- c(first.db.name, second.db.name)

cat("Loading data from both databases, please wait...\n")

all.data <- parSapply(cl, 
                      db.names, 
                      simplify = FALSE,
                      function(db.name) {
                        db.conn <- NA
                        if (file_ext(db.name) == "accdb") {
                          db.conn <- odbcConnectAccess2007(db.name)
                        } else {
                          db.conn <- odbcConnectAccess(db.name)
                        }
                        data <- getBaseRecoveries(db.conn)
                        odbcClose(db.conn)
                        return(data)
                      })
stopCluster(cl)

first.df <- 
  as_tibble(all.data[[1]]) %>%
  mutate(RecoveryMonth = as.integer(month(RecoveryDate)),
         RecTagCode =  str_c("'", TagCode)) %>%
  select(one_of(kUsedFields))

second.df <-
  as_tibble(all.data[[2]]) %>%
  mutate(RecoveryMonth = as.integer(month(RecoveryDate)),
         RecTagCode = str_c("'",TagCode)) %>%
  select(one_of(kUsedFields))

compare.df <- full_join(first.df, second.df, by=kKeyFields)

# merge the Auxiliary flag, Recovery Month, Recovery Tag Code, and Fishery Name 
# from the first and second data set 
compare.df <-
  compare.df %>%
  mutate(Auxiliary = coalesce(Auxiliary.y, Auxiliary.x),
         RecordFisheryName = coalesce(FisheryName.y, FisheryName.x),
         RecoveryMonth = coalesce(RecoveryMonth.y, RecoveryMonth.x),
         RecoverySite = coalesce(RecoverySite.y, RecoverySite.x),
         RecTagCode = coalesce(RecTagCode.y, RecTagCode.x)) %>%
  select(-one_of("Auxiliary.x", "Auxiliary.y", 
                 "FisheryName.y", "FisheryName.x", 
                 "RecoveryMonth.y", "RecoveryMonth.x",
                 "RecTagCode.y", "RecTagCode.x"))

modified.df <- NULL

#Find Estimated Number Changes
cat("Checking for Tag Code differences...\n")
estimate.modifed.df <- 
  compare.df %>%
  filter(EstimatedNumber.x != EstimatedNumber.y) %>%
  select(one_of(c(kKeyFields, 
                  kPassThroughFields, 
                  "RecordFisheryName", 
                  "EstimatedNumber.x", 
                  "EstimatedNumber.y"))) %>%
  mutate_at(vars("EstimatedNumber.x", "EstimatedNumber.y"), as.character) %>%
  rename(FirstValue = EstimatedNumber.x,
         SecondValue = EstimatedNumber.y) %>%
  mutate(FieldName = "EstimatedNumber",
         Comment = "")

modified.df <- rbind(modified.df, estimate.modifed.df)

#Find Fishery Name Changes
cat("Checking for Fishery Name differences...\n")
fishery.modifed.df <- 
  compare.df %>%
  filter(coalesce(FisheryName.x, "") != coalesce(FisheryName.y, "")) %>%
  select(one_of(c(kKeyFields, 
                  kPassThroughFields, 
                  "RecordFisheryName", 
                  "FisheryName.x", 
                  "FisheryName.y"))) %>%
  rename(fishery.modifed.df, 
         FirstValue=FisheryName.x,
         SecondValue=FisheryName.y) %>%
  mutate(FieldName = "FisheryName",
         Comment = "")

modified.df <- rbind(modified.df, fishery.modifed.df)

#Find Tag Code Changes
cat("Checking for Tag Code differences...\n")
tag.modifed.df <- 
  compare.df %>%
  filter(TagCode.x != TagCode.y) %>%
  select(one_of(c(kKeyFields, 
                  kPassThroughFields, 
                  "RecordFisheryName", 
                  "TagCode.x", 
                  "TagCode.y"))) %>%
  mutate(TagCode.x = str_c("'", TagCode.x),
         TagCode.y = str_c("'", TagCode.y)) %>%
  rename(FirstValue=TagCode.x,
         SecondValue=TagCode.y) %>%
  mutate(FieldName = "TagCode",
         Comment = "")

modified.df <- rbind(modified.df, tag.modifed.df)

#Find Added Records
cat("Identifying Added Recoveries...\n")
modified.df <- 
  compare.df %>%
  filter(is.na(TagCode.x) == TRUE, is.na(TagCode.y) == FALSE) %>%
  select(one_of(c(kKeyFields, 
                  kPassThroughFields,
                  "RecordFisheryName"))) %>%
  mutate(FirstValue = "",
         SecondValue = "",
         FieldName = "",
         Comment = as.character(str_glue("Added recovery to {basename(db.names[2])}"))) %>%
  bind_rows(modified.df)

#Find Removed Records
cat("Identifying Removed Recoveries...\n")
modified.df <- 
  compare.df %>%
  filter(is.na(TagCode.x) == FALSE, is.na(TagCode.y) == TRUE) %>%
  select(one_of(c(kKeyFields, 
                  kPassThroughFields,
                  "RecordFisheryName"))) %>%
  mutate(FirstValue = "",
         SecondValue = "",
         FieldName = "",
         Comment = str_glue("Removed recovery from {basename(db.names[2])}")) %>%
  bind_rows(modified.df) %>%
  arrange(RunYear, Agency, RecoveryId)


diff.file.name <- file.path(report.dir, str_glue("diff_{GetTimeStampText()}.csv"))
WriteCsv(diff.file.name, modified.df)

cat(str_glue("\nYour difference report file is now available at:\n{normalizePath(diff.file.name)}\n\n"))
cat("Done\n")




