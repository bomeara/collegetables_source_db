library(targets)

source("functions.R")
source("_packages.R")
options(timeout=24*60*60) # let things download for at least 24 hours (important while on slow internet connection)
options(download.file.method = "libcurl")

list(
 tar_target(min_year, 2012),
 tar_target(created_db, CreateDB(min_year=min_year)),
 tar_target(sensible_db, ProcessIntoSensibleDatabase(created_db)), # input is completion time of the previous step, so it only runs if previous one changes, without needing to pass in anything fancy
 tar_target(college_data_raw, CreateCollegeDataAllYears(sensible_db)),
 tar_target(college_data, AggregatePercentages(college_data_raw)),
 tar_target(college_data_most_recent, GetLatestYear(college_data))
)