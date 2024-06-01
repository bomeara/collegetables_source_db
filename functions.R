CreateDB(min_year=2004) { 
	temp_dir <- tempdir()
	for (year in min_year:get(Sys.Date()$year)) {
		# download the data
		try({
			destination <- paste0(temp_dir, "/IPEDS_", year, "-", year+1, ".zip")
			source <- paste0("https://nces.ed.gov/ipeds/tablefiles/zipfiles/IPEDS_", year, "-", year+1-2000, "_Final.zip")
			download.file(url=source, destfile=destination)
			unzip(destination, exdir=temp_dir)
			table_file <- paste0(temp_dir, "/IPEDS", year, -2000+year+1, ".accdb")
			all_tables <- mdbr::mdb_tables(table_file)
			overview_table<- mdbr::read_mdb(table_file, all_tables[grepl("Tables", all_tables)])
			var_table <- mdbr::read_mdb(table_file, all_tables[grepl("vartable", all_tables)])
			valueSet_table <- mdbr::read_mdb(table_file, all_tables[grepl("valuesets", all_tables)])
			for (i in sequence(nrow(overview_table))) {
				focal_table <- mdbr::read_mdb(table_file, overview_table$TableName[i])
					
			}

		})
		
	}
}

