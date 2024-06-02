library(targets)

source("functions.R")
source("_packages.R")
options(timeout=24*60*60) # let things download for at least 24 hours (important while on slow internet connection)
options(download.file.method = "libcurl")

list(
 tar_target(min_year, 2012),
 tar_target(created_db, CreateDB(min_year=min_year))
)