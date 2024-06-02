CreateDB <- function(min_year=2004) { 
	try({file.remove("data/db_IPEDS.sqlite")}, silent=TRUE)
	db <- dbConnect(RSQLite::SQLite(), "data/db_IPEDS.sqlite")
	temp_dir <- tempdir()
	for (year in min_year:as.integer(format(Sys.Date(), "%Y"))) {
		# download the data
		try({
			print(year)
			destination <- paste0(temp_dir, "/IPEDS_", year, "-", year+1, ".zip")
			source <- paste0("https://nces.ed.gov/ipeds/tablefiles/zipfiles/IPEDS_", year, "-", year+1-2000, "_Final.zip")
			source_provisional <- paste0("https://nces.ed.gov/ipeds/tablefiles/zipfiles/IPEDS_", year, "-", year+1-2000, "_Provisional.zip")
			tryCatch(download.file(url=source, destfile=destination), error=function(e) download.file(url=source_provisional, destfile=destination))
			unzip(destination, exdir=temp_dir)
			table_file <- paste0(temp_dir, "/IPEDS", year, -2000+year+1, ".accdb")
			all_tables <- mdbr::mdb_tables(table_file)
			overview_table<- mdbr::read_mdb(table_file, all_tables[grepl("Tables", all_tables)])
			var_table <- mdbr::read_mdb(table_file, all_tables[grepl("vartable", all_tables)])
			valueSet_table <- mdbr::read_mdb(table_file, all_tables[grepl("valuesets", all_tables)])
			for (i in sequence(nrow(overview_table))) {
				focal_table_name <- overview_table$TableName[i]
				print(focal_table_name)
				focal_table <- mdbr::read_mdb(table_file, focal_table_name, col_types= list(.default = readr::col_character()))
				focal_variables <- subset(var_table, TableName==focal_table_name)
				focal_valuesets <- subset(valueSet_table, TableName==focal_table_name)
				conversion_columns <- unique(focal_valuesets$varName)
				for (conversion_column_name in conversion_columns) {	
					try({
						focal_table_col_index <- which(colnames(focal_table)==conversion_column_name)
						if(length(focal_table_col_index)==0) {
							next
						}
						conversion_table <- subset(focal_valuesets, varName==conversion_column_name)
						for (j in sequence(nrow(conversion_table))) {
							conversion_value <- conversion_table$Codevalue[j]
							if(is.na(conversion_value)) {
								next
							}
							conversion_label <- conversion_table$valueLabel[j]
							focal_table[focal_table[,focal_table_col_index]==conversion_value, focal_table_col_index] <- paste0(conversion_value, ": ", conversion_label)
						}
					})
				}
				focal_table$ReportYear <- year
				focal_table$TableName <- focal_table_name
				focal_table_name_no_year <- gsub("\\d\\d\\d\\d", "zzzz", focal_table_name)
				colnames(focal_table) <- toupper(colnames(focal_table)) # b/c sometimes they have unitid and others UNITID
				original_table <- data.frame()
				try({original_table <- dbReadTable(conn=db, name=focal_table_name_no_year)}, silent=TRUE)
				if(nrow(original_table)==0) {
					dbWriteTable(conn=db,  name=focal_table_name_no_year, value=focal_table, overwrite=TRUE, append=FALSE)
				} else {
					dbWriteTable(conn=db,  name=focal_table_name_no_year, value=dplyr::bind_rows(original_table, focal_table), overwrite=TRUE, append=FALSE)
				}
			}
			var_table$ReportYear <- year
			valueSet_table$ReportYear <- year
			colnames(var_table) <- toupper(colnames(var_table))
			colnames(valueSet_table) <- toupper(colnames(valueSet_table))
			var_table_orig <- data.frame()
			valueSet_table_orig <- data.frame()
			try({var_table_orig <- dbReadTable(conn=db, name="var_table")}, silent=TRUE)
			try({valueSet_table_orig <- dbReadTable(conn=db, name="valueSet_table")}, silent=TRUE)
			if(nrow(var_table_orig)==0) {
				dbWriteTable(conn=db,  name="var_table", value=var_table, overwrite=FALSE, append=TRUE)
			} else {
				dbWriteTable(conn=db,  name="var_table", value=dplyr::bind_rows(var_table_orig, var_table), overwrite=FALSE, append=TRUE)
			}
			if(nrow(valueSet_table_orig)==0) {
				dbWriteTable(conn=db,  name="valueSet_table", value=valueSet_table, overwrite=FALSE, append=TRUE)
			} else {
				dbWriteTable(conn=db,  name="valueSet_table", value=dplyr::bind_rows(valueSet_table_orig, valueSet_table), overwrite=FALSE, append=TRUE)
			}

		})
		
	}
	dbDisconnect(db)
	return("data/db_IPEDS.sqlite")
}

CompressTableTypes <- function() {
	db <- dbConnect(RSQLite::SQLite(), "data/db_IPEDS.sqlite")
	table_info <- dbReadTable(conn=db, name="var_table")
	table_info <- table_info|> mutate(TABLETITLE=gsub("\\d\\d\\d\\d-\\d\\d", "zzzz-zz", TABLETITLE)) |> mutate(TABLETITLE=gsub("\\d\\d\\d\\d", "zzzz", TABLETITLE))
	# get rid of duplicates 
}