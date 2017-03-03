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
header <- "PSC CAS Base Recovery Compare v0.1"

kKeyFields <- c("RecoveryId", "Agency", "RunYear")
kPassThroughFields <- c("Auxiliary")
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

required.packages <- c("RODBC", "dplyr", "parallel")
InstallRequiredPackages(required.packages)

SetupCluster <- function() {
  cl <- makeCluster(2)
  clusterEvalQ(cl, library(RODBC))
  clusterExport(cl, c("cas.db.lib.name", "util.lib.name"))
  clusterEvalQ(cl, source(util.lib.name))
  clusterEvalQ(cl, source(cas.db.lib.name))
  clusterEvalQ(cl, options(stringsAsFactors = FALSE))
  return(cl)
}


cat(paste0(header, "\n"))
first.db.name <- choose.files(caption = "Select First CAS Database file", multi=FALSE, filters = Filters[c("txt", "All"),])

if (length(first.db.name) == 0) {
  stop("Selecting first database was cancelled by the user.")
}

second.db.name <- choose.files(caption = "Select Second CAS Database file", multi=FALSE, filters = Filters[c("txt", "All"),])

if (length(second.db.name) == 0) {
  stop("Selecting second database was cancelled by the user.")
}

cl <- SetupCluster()

db.names <- c(first.db.name, second.db.name)

cat("Loading data from both databases, please wait...\n")

all.data <- parSapply(cl, 
                      db.names, 
                      simplify = FALSE,
                      function(db.name) {
                        db.conn <- odbcConnectAccess(db.name)
                        data <- GetCasBaseRecoveries(db.conn)
                        odbcClose(db.conn)
                        return(data)
                      })
stopCluster(cl)

first.df <- all.data[[1]] 
second.df <- all.data[[2]]

first.df <- select(first.df, one_of(kUsedFields))
second.df <- select(second.df, one_of(kUsedFields))

compare.df <- full_join(first.df, second.df, by=kKeyFields)

#merge the Auxiliary flag from the first and second data set to a single value in Auxiliary
compare.df$Auxiliary.x[is.na(compare.df$Auxiliary.x)]  <- compare.df$Auxiliary.y[is.na(compare.df$Auxiliary.x)]
compare.df <- select(compare.df, -one_of("Auxiliary.y"))
compare.df <- rename(compare.df, Auxiliary=Auxiliary.x)

#merge the Fishery name from the first and second data set to a single value in FisheryName
compare.df$RecordFisheryName <- compare.df$FisheryName.y
compare.df$RecordFisheryName[is.na(compare.df$RecordFisheryName)]  <- compare.df$FisheryName.x[is.na(compare.df$RecordFisheryName)]



modified.df <- NULL

#Find Estimated Number Changes
cat("Checking for Tag Code differences...\n")
estimate.modifed.df <- filter(compare.df, EstimatedNumber.x != EstimatedNumber.y)

estimate.modifed.df <- select(estimate.modifed.df, 
                              one_of(c(kKeyFields, 
                                       "Auxiliary", 
                                       "RecordFisheryName", 
                                       "EstimatedNumber.x", 
                                       "EstimatedNumber.y")))

estimate.modifed.df$EstimatedNumber.x <- as.character(estimate.modifed.df$EstimatedNumber.x)
estimate.modifed.df$EstimatedNumber.y <- as.character(estimate.modifed.df$EstimatedNumber.y)
estimate.modifed.df <- rename(estimate.modifed.df, 
                              FirstValue=EstimatedNumber.x,
                              SecondValue=EstimatedNumber.y)
estimate.modifed.df$FieldName <- "EstimatedNumber"
estimate.modifed.df$Comment <- ""

modified.df <- rbind(modified.df, estimate.modifed.df)

#Find Fishery Name Changes
cat("Checking for Fishery Name differences...\n")
fishery.modifed.df <- filter(compare.df, FisheryName.x != FisheryName.y)
fishery.modifed.df <- select(fishery.modifed.df, 
                              one_of(c(kKeyFields, 
                                       "Auxiliary", 
                                       "RecordFisheryName", 
                                       "FisheryName.x", 
                                       "FisheryName.y")))

fishery.modifed.df <- rename(fishery.modifed.df, 
                              FirstValue=FisheryName.x,
                              SecondValue=FisheryName.y)
fishery.modifed.df$FieldName <- "FisheryName"
fishery.modifed.df$Comment <- ""
modified.df <- rbind(modified.df, fishery.modifed.df)

#Find Tag Code Changes
cat("Checking for Tag Code differences...\n")
tag.modifed.df <- filter(compare.df, TagCode.x != TagCode.y)
tag.modifed.df <- select(tag.modifed.df, 
                             one_of(c(kKeyFields, 
                                      "Auxiliary", 
                                      "RecordFisheryName", 
                                      "TagCode.x", 
                                      "TagCode.y")))

tag.modifed.df$TagCode.x <- paste0("'", tag.modifed.df$TagCode.x)
tag.modifed.df$TagCode.y <- paste0("'", tag.modifed.df$TagCode.y)
tag.modifed.df <- rename(tag.modifed.df, 
                             FirstValue=TagCode.x,
                             SecondValue=TagCode.y)

tag.modifed.df$FieldName <- "TagCode"
tag.modifed.df$Comment <- ""
modified.df <- rbind(modified.df, tag.modifed.df)

#Find Added Records
cat("Identifying Added Recoveries...\n")
added.records.df <- filter(compare.df, is.na(TagCode.x) == TRUE, is.na(TagCode.y) == FALSE)
added.records.df <- select(added.records.df, 
                         one_of(c(kKeyFields, 
                                  "Auxiliary",
                                  "RecordFisheryName")))

added.records.df$FirstValue <- ""
added.records.df$SecondValue <- ""
added.records.df$FieldName <- ""
added.records.df$Comment <- "Added recovery to second database"

modified.df <- rbind(modified.df, added.records.df)

#Find Removed Records
cat("Identifying Removed Recoveries...\n")
removed.records.df <- filter(compare.df, is.na(TagCode.x) == FALSE, is.na(TagCode.y) == TRUE)
removed.records.df <- select(removed.records.df, 
                             one_of(c(kKeyFields, 
                                      "Auxiliary",
                                      "RecordFisheryName")))

removed.records.df$FirstValue <- ""
removed.records.df$SecondValue <- ""
removed.records.df$FieldName <- ""
removed.records.df$Comment <- "Removed recovery from the second database"
modified.df <- rbind(modified.df, removed.records.df)

modified.df <- arrange(modified.df, RunYear, Agency, RecoveryId)


diff.file.name <- file.path(report.dir,sprintf("diff_%s.csv", GetTimeStampText()))
WriteCsv(diff.file.name, modified.df)

cat(sprintf("\nYour difference report file is now available at:\n%s\n\n", normalizePath(diff.file.name)))
cat("Done\n")




