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
				for (col_index in sequence(ncol(focal_table))) {
					if(grepl("UNITID", colnames(focal_table)[col_index], ignore.case=TRUE)) {
						colnames(focal_table)[col_index] <- toupper(colnames(focal_table)[col_index])
						next
					}
					matching_variable <- subset(focal_variables, varName==colnames(focal_table)[col_index])
					if(nrow(matching_variable)==1) {
						colnames(focal_table)[col_index] <- toupper(make.names(paste0(colnames(focal_table)[col_index], "___",  matching_variable$varTitle))) 
						# They drop ALIEN at times from nonresident, remove to be consistent; remove trailing period
					} 
					colnames(focal_table)[col_index] <- gsub("\\.$", "", gsub("NONRESIDENT\\.ALIEN", "U.S..NONRESIDENT", colnames(focal_table)[col_index]))
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
				dbWriteTable(conn=db,  name="var_table", value=var_table, overwrite=TRUE, append=FALSE)
			} else {
				dbWriteTable(conn=db,  name="var_table", value=dplyr::bind_rows(var_table_orig, var_table), overwrite=TRUE, append=FALSE)
			}
			if(nrow(valueSet_table_orig)==0) {
				dbWriteTable(conn=db,  name="valueSet_table", value=valueSet_table, overwrite=TRUE, append=FALSE)
			} else {
				dbWriteTable(conn=db,  name="valueSet_table", value=dplyr::bind_rows(valueSet_table_orig, valueSet_table), overwrite=TRUE, append=FALSE)
			}

		})
		
	}
	dbDisconnect(db)
	return("data/db_IPEDS.sqlite")
}



# Take the most important tables, process them.
ProcessIntoSensibleDatabase <- function(db_source) {
	db <- dbConnect(RSQLite::SQLite(), db_source)
	try({file.remove("data/db_sensible.sqlite")}, silent=TRUE)
	dbs <- dbConnect(RSQLite::SQLite(), "data/db_sensible.sqlite")
	
	# degrees by major and demographics
	Czzzz_a <- dbReadTable(conn=db, name="Czzzz_a")
	Czzzz_a$`CIP.CODE` <- NA
	Czzzz_a$`CIP.CODE` <- Czzzz_a$`CIPCODE___CIP.CODE....2020.CLASSIFICATION`
	Czzzz_a$`CIP.CODE`[is.na(Czzzz_a$`CIP.CODE`)] <- Czzzz_a$`CIPCODE___CIP.CODE....2010.CLASSIFICATION`[is.na(Czzzz_a$`CIP.CODE`)]
	Czzzz_a <- Czzzz_a |> dplyr::select(-dplyr::ends_with("CLASSIFICATION"))
	Czzzz_a_tall <- Czzzz_a |> pivot_longer(cols=-c(UNITID, CIP.CODE, MAJORNUM___FIRST.OR.SECOND.MAJOR, AWLEVEL___AWARD.LEVEL.CODE, REPORTYEAR, TABLENAME), names_to="GRADUATES.BY.MAJOR.CATEGORY", values_to="GRADUATES.BY.MAJOR.CATEGORY.COUNT")
	Czzzz_a_tall$`CIP.LEVEL` <- NA
	Czzzz_a_tall$`CIP.LEVEL`[grepl("^\\d\\d:", Czzzz_a_tall$`CIP.CODE`)] <- "1"
	Czzzz_a_tall$`CIP.LEVEL`[grepl("^\\d\\d\\.\\d\\d:", Czzzz_a_tall$`CIP.CODE`)] <- "2"
	Czzzz_a_tall$`CIP.LEVEL`[grepl("^\\d\\d\\.\\d\\d\\d\\d:", Czzzz_a_tall$`CIP.CODE`)] <- "3"
	dbWriteTable(conn=dbs, name="Majors", value=Czzzz_a_tall, overwrite=TRUE, append=FALSE)
	
	# institutional directory
	HDzzzz <- dbReadTable(conn=db, name="HDzzzz")
	
	GetLeadingNumber <- function(x) {
		return(gsub(":.*", "", x))
	}
	
	carnegie_categories <- c("BASIC", "UNDERGRADUATE.INSTRUCTIONAL.PROGRAM", "\\.GRADUATE.INSTRUCTIONAL.PROGRAM", "UNDERGRADUATE.PROFILE", "ENROLLMENT.PROFILE", "SIZE.AND.SETTING") # Need the backslashes to escape the period for Graduate, otherwise it matches both undergraduate and graduate
	
	for (carnegie_category in carnegie_categories) {
		print(carnegie_category)
		try({
			new_col_name <- paste0("CARNEGIE.", carnegie_category)
			new_col_name <- gsub("\\\\\\.+", "", new_col_name)
			focal_columns <- which(grepl(paste0(carnegie_category, '$'), colnames(HDzzzz)))
			focal_column_names <- colnames(HDzzzz)[focal_columns]
			latest_column_name <- focal_column_names[grepl("2021", focal_column_names)]
			entries <- gsub("\\r", "", gsub("\\n", "", unique(HDzzzz[[latest_column_name]])))
			conversion_data_frame <- data.frame(id=GetLeadingNumber(entries), label=entries)
			HDzzzz[[new_col_name]] <- HDzzzz[[latest_column_name]]
			for (focal_column_name in focal_column_names) {
				if(focal_column_name==latest_column_name) {
					next
				}
				HDzzzz[[new_col_name]][is.na(HDzzzz[[new_col_name]])] <- HDzzzz[[focal_column_name]][is.na(HDzzzz[[new_col_name]])]
			}
			numbers <- GetLeadingNumber(HDzzzz[[new_col_name]])
			unique_numbers <- unique(numbers)
			unique_numbers <- unique_numbers[!is.na(unique_numbers)]
			for (unique_number in unique_numbers) {
				unique_number_entries <- conversion_data_frame[conversion_data_frame$id==unique_number, "label"]
				unique_number_entries <- unique_number_entries[!is.na(unique_number_entries)]
				if(length(unique_number_entries)==0) {
					next
				}
				HDzzzz[[new_col_name]][numbers==unique_number] <- unique_number_entries
			}	
		})
	}
	
	dbWriteTable(conn=dbs, name="InstitutionalDirectory", value=HDzzzz, overwrite=TRUE, append=FALSE)
	rm(HDzzzz)

	# institutional offerings
	Iczzzz <- dbReadTable(conn=db, name="Iczzzz")
	dbWriteTable(conn=dbs, name="InstitutionalOfferings", value=Iczzzz, overwrite=TRUE, append=FALSE)
	rm(ICzzzz)
	
	# institutional charges
	ICzzzz_AY <- dbReadTable(conn=db, name="ICzzzz_AY")
	ICzzzz_AY_selected <- ICzzzz_AY |> dplyr::select("UNITID", "REPORTYEAR", "TABLENAME", dplyr::contains("TUITION"))
	dbWriteTable(conn=dbs, name="InstitutionalCharges", value=ICzzzz_AY_selected, overwrite=TRUE, append=FALSE)
	rm(ICzzzz_AY)
	rm(ICzzzz_AY_selected)
	
	# twelve month headcount
	EFFYzzzz <- dbReadTable(conn=db, name="EFFYzzzz")
	dbWriteTable(conn=dbs, name="TwelveMonthHeadcount", value=EFFYzzzz, overwrite=TRUE, append=FALSE)
	rm(EFFYzzzz)
	
	# admissions
	ADMzzzz <- dbReadTable(conn=db, name="ADMzzzz")
	dbWriteTable(conn=dbs, name="Admissions", value=ADMzzzz, overwrite=TRUE, append=FALSE)
	rm(ADMzzzz)
	
	# enrollment race
	EFzzzzA <- dbReadTable(conn=db, name="EFzzzzA")
	dbWriteTable(conn=dbs, name="EnrollmentRace", value=EFzzzzA, overwrite=TRUE, append=FALSE)
	rm(EFzzzzA)
	
	# enrollment major
	EFzzzzCP <- dbReadTable(conn=db, name="EFzzzzCP")
	dbWriteTable(conn=dbs, name="EnrollmentMajor", value=EFzzzzCP, overwrite=TRUE, append=FALSE)
	rm(EFzzzzCP)
	
	# enrollment age
	EFzzzzB <- dbReadTable(conn=db, name="EFzzzzB")
	dbWriteTable(conn=dbs, name="EnrollmentAge", value=EFzzzzB, overwrite=TRUE, append=FALSE)
	rm(EFzzzzB)
	
	# enrollment residence
	EFzzzzC <- dbReadTable(conn=db, name="EFzzzzC")
	dbWriteTable(conn=dbs, name="EnrollmentResidence", value=EFzzzzC, overwrite=TRUE, append=FALSE)
	rm(EFzzzzC)
	
	# completions by race
	Czzzz_B <- dbReadTable(conn=db, name="Czzzz_B")
	colnames(Czzzz_B) <- gsub("\\.$", "", gsub("\\.$", "", (colnames(Czzzz_B))))
	dbWriteTable(conn=dbs, name="CompletionsRace", value=Czzzz_B, overwrite=TRUE, append=FALSE)
	rm(Czzzz_B)
	
	# completions by age
	Czzzz_C <- dbReadTable(conn=db, name="Czzzz_C")
	colnames(Czzzz_C) <- gsub("\\.$", "", gsub("\\.$", "", (colnames(Czzzz_C)))
	dbWriteTable(conn=dbs, name="CompletionsAge", value=Czzzz_C, overwrite=TRUE, append=FALSE)
	rm(Czzzz_C)
	
	# instructional gender
	SALzzzz_IS <- dbReadTable(conn=db, name="SALzzzz_IS")
	dbWriteTable(conn=dbs, name="InstructionalGender", value=SALzzzz_IS, overwrite=TRUE, append=FALSE)
	rm(SALzzzz_IS)
	
	# non-instructional staff
	SALzzzz_NIS <- dbReadTable(conn=db, name="SALzzzz_NIS")
	dbWriteTable(conn=dbs, name="NonInstructionalStaff", value=SALzzzz_NIS, overwrite=TRUE, append=FALSE)
	rm(SALzzzz_NIS)
	
	# staff category
	Szzzz_OC <- dbReadTable(conn=db, name="Szzzz_OC")
	dbWriteTable(conn=dbs, name="StaffCategory", value=Szzzz_OC, overwrite=TRUE, append=FALSE)
	rm(Szzzz_OC)
	
	#staff tenure category
	Szzzz_SIS <- dbReadTable(conn=db, name="Szzzz_SIS")
	dbWriteTable(conn=dbs, name="StaffTenureCategory", value=Szzzz_SIS, overwrite=TRUE, append=FALSE)
	rm(Szzzz_SIS)
	
	# staff gender
	Szzzz_IS <- dbReadTable(conn=db, name="Szzzz_IS")
	dbWriteTable(conn=dbs, name="StaffGender", value=Szzzz_IS, overwrite=TRUE, append=FALSE)
	rm(Szzzz_IS)
	
	# staff new
	Szzzz_NH <- dbReadTable(conn=db, name="Szzzz_NH")
	dbWriteTable(conn=dbs, name="StaffNew", value=Szzzz_NH, overwrite=TRUE, append=FALSE)
	rm(Szzzz_NH)
	
	# employees position
	EAPzzzz <- dbReadTable(conn=db, name="EAPzzzz")
	dbWriteTable(conn=dbs, name="EmployeesPosition", value=EAPzzzz, overwrite=TRUE, append=FALSE)
	rm(EAPzzzz)
	
	# student aid overview
	SFAzzzz_P1 <- dbReadTable(conn=db, name="SFAzzzz_P1")
	dbWriteTable(conn=dbs, name="StudentAidOverview", value=SFAzzzz_P1, overwrite=TRUE, append=FALSE)
	rm(SFAzzzz_P1)

	# yearly results
	SFAzzzz_P2 <- dbReadTable(conn=db, name="SFAzzzz_P2")
	financial_aid_by_year <- data.frame()
	year_data_indices <- which(grepl("20\\d\\d\\.\\d\\d$", colnames(SFAzzzz_P2)))
	for (year_data_index in year_data_indices) {
		full_title <- strsplit(colnames(SFAzzzz_P2)[year_data_index], "___")[[1]][2]
		actual_year <- as.numeric(substr(substr(full_title, nchar(full_title)-6, nchar(full_title)),1,4))
		actual_title <- gsub("\\.$", "", substr(full_title, 1, nchar(full_title)-9))
		local_df <- data.frame(UNITID=SFAzzzz_P2$UNITID, REPORTYEAR=SFAzzzz_P2$REPORTYEAR, ACTUALYEAR=actual_year, TABLENAME=SFAzzzz_P2$TABLENAME, COLUMN=actual_title, VALUE=SFAzzzz_P2[[year_data_index]])
		local_df <- local_df[!is.na(local_df$VALUE),]
		local_df <- local_df[local_df$REPORTYEAR-local_df$ACTUALYEAR<2,]
		financial_aid_by_year <- dplyr::bind_rows(financial_aid_by_year, local_df)
		print(nrow(financial_aid_by_year))
		print(year_data_index)
	}
	dbWriteTable(conn=dbs, name="StudentAidYearly", value=financial_aid_by_year, overwrite=TRUE, append=FALSE)
	
	# graduation data four years
	GRzzzz <- dbReadTable(conn=db, name="GRzzzz")
	dbWriteTable(conn=dbs, name="GraduationFourYear", value=GRzzzz, overwrite=TRUE, append=FALSE)
	rm(GRzzzz)
	
	# graduation data L2 
	GRzzzz_L2 <- dbReadTable(conn=db, name="GRzzzz_L2")
	dbWriteTable(conn=dbs, name="GraduationTwoYear", value=GRzzzz_L2, overwrite=TRUE, append=FALSE)
	rm(GRzzzz_L2)
	
	# graduation data PELL
	GRzzzz_PELL_SSL <- dbReadTable(conn=db, name="GRzzzz_PELL_SSL")
	dbWriteTable(conn=dbs, name="GraduationPELL", value=GRzzzz_PELL_SSL, overwrite=TRUE, append=FALSE)
	rm(GRzzzz_PELL_SSL)
	
	# library data
	ALzzzz <- dbReadTable(conn=db, name="ALzzzz")
	colname_bases <- gsub("___.*$", "", colnames(ALzzzz))
	ALzzzz_clean <- ALzzzz[,which(!duplicated(colname_bases))]
	colname_bases_table <- table(colname_bases)
	multiple_bases <- names(colname_bases_table[colname_bases_table>1])
	for (multiple_base in multiple_bases) {
		matching_columns_sources <- which(grepl(multiple_base, colnames(ALzzzz)))
		matching_columns_destination <- which(grepl(multiple_base, colnames(ALzzzz_clean)))
		for (multiple_base_index in matching_columns_sources) {
			ALzzzz_clean[is.na(ALzzzz_clean[,matching_columns_destination]), matching_columns_destination] <- ALzzzz[is.na(ALzzzz_clean[,matching_columns_destination]), multiple_base_index]
		}
	}
	dbWriteTable(conn=dbs, name="Library", value=ALzzzz_clean, overwrite=TRUE, append=FALSE)
	rm(ALzzzz)
	rm(ALzzzz_clean)

	# comparison table 
	CUSTOMCGIDSzzzz <- dbReadTable(conn=db, name="CUSTOMCGIDSzzzz")
	dbWriteTable(conn=dbs, name="ComparisonInstitutionTable", value=CUSTOMCGIDSzzzz, overwrite=TRUE, append=FALSE)
	rm(CUSTOMCGIDSzzzz)
	
	

	
	
	
	


	
	
	return(1)
}

CreateNetwork <- function() {
	# Convert ComparisonInstitutionTable into a network
	dbs <- dbConnect(RSQLite::SQLite(), "data/db_sensible.sqlite")
	ComparisonInstitutionTable <- dbReadTable(conn=dbs, name="ComparisonInstitutionTable")
	ComparisonInstitutionTable$FromTo <- paste0(ComparisonInstitutionTable$UNITID, "___", ComparisonInstitutionTable$CGUNITID)
	ComparisonInstitutionTableFiltered <- ComparisonInstitutionTable[!duplicated(ComparisonInstitutionTable$FromTo),]
	ComparisonInstitutionTableFiltered <- ComparisonInstitutionTableFiltered[!is.na(ComparisonInstitutionTableFiltered$UNITID),]
	ComparisonInstitutionTableFiltered <- ComparisonInstitutionTableFiltered[!is.na(ComparisonInstitutionTableFiltered$CGUNITID),]
	ComparisonInput <- data.frame(from=ComparisonInstitutionTableFiltered$UNITID, to=ComparisonInstitutionTableFiltered$CGUNITID)
	g <- igraph::graph_from_data_frame(ComparisonInput, directed=TRUE)
	g_distances <- distances(g)
	

	
}