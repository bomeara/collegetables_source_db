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
			all_tables <- c()
			try({all_tables <- mdbr::mdb_tables(table_file)})
			if(length(all_tables)<3) { # handle case of it opening inside another folder
				all_files <- list.files(temp_dir, recursive=TRUE, full.names=TRUE)
				table_file <- all_files[grepl(paste0("IPEDS", year, -2000+year+1, ".accdb"), all_files)]
				try({all_tables <- mdbr::mdb_tables(table_file)})
			}
			overview_table_name <- all_tables[grepl("Tables", all_tables)]
			overview_table_name <- overview_table_name[!grepl("RV", overview_table_name)]
			overview_table<- mdbr::read_mdb(table_file, overview_table_name)
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
				focal_table_name_no_year <- gsub("x", "z", focal_table_name_no_year) # to handle a table named EFxxxxCP in 2021 dats

				colnames(focal_table) <- toupper(colnames(focal_table)) # b/c sometimes they have unitid and others UNITID
				original_table <- data.frame()
				try({original_table <- dbReadTable(conn=db, name=focal_table_name_no_year)}, silent=TRUE)
				
				if(focal_table_name_no_year=="SFAzzzz_P2") { # this gets very wide as each year gets its own columns, need to process earlier
					SFAzzzz_P2 <- focal_table
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
					}
					focal_table <- financial_aid_by_year
				}
				
				
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
	return(Sys.time())
}



# Take the most important tables, process them.
ProcessIntoSensibleDatabase <- function(finish_time) {
	db <- dbConnect(RSQLite::SQLite(), "data/db_IPEDS.sqlite")
	try({file.remove("data/db_sensible.sqlite")}, silent=TRUE)
	dbs <- dbConnect(RSQLite::SQLite(), "data/db_sensible.sqlite")
	
	print("Starting individual tables")
	
	# degrees by major and demographics
	Czzzz_a <- dbReadTable(conn=db, name="Czzzz_a")
	print("Loaded Czzzz_a")
	Czzzz_a$`CIP.CODE` <- NA
	Czzzz_a$`CIP.CODE` <- Czzzz_a$`CIPCODE___CIP.CODE....2020.CLASSIFICATION`
	Czzzz_a$`CIP.CODE`[is.na(Czzzz_a$`CIP.CODE`)] <- Czzzz_a$`CIPCODE___CIP.CODE....2010.CLASSIFICATION`[is.na(Czzzz_a$`CIP.CODE`)]
	Czzzz_a <- Czzzz_a |> dplyr::select(-dplyr::ends_with("CLASSIFICATION"))
	Czzzz_a_tall <- Czzzz_a |> pivot_longer(cols=-c(UNITID, CIP.CODE, MAJORNUM___FIRST.OR.SECOND.MAJOR, AWLEVEL___AWARD.LEVEL.CODE, REPORTYEAR, TABLENAME), names_to="GRADUATES.BY.MAJOR.CATEGORY", values_to="GRADUATES.BY.MAJOR.CATEGORY.COUNT")
	print("Pivoted Czzzz_a")
	Czzzz_a_tall$`CIP.LEVEL` <- NA
	Czzzz_a_tall$`CIP.LEVEL`[grepl("^\\d\\d:", Czzzz_a_tall$`CIP.CODE`)] <- "1"
	Czzzz_a_tall$`CIP.LEVEL`[grepl("^\\d\\d\\.\\d\\d:", Czzzz_a_tall$`CIP.CODE`)] <- "2"
	Czzzz_a_tall$`CIP.LEVEL`[grepl("^\\d\\d\\.\\d\\d\\d\\d:", Czzzz_a_tall$`CIP.CODE`)] <- "3"
	dbWriteTable(conn=dbs, name="Majors", value=Czzzz_a_tall, overwrite=TRUE, append=FALSE)
	print("Majors")
	
	# institutional directory
	HDzzzz <- dbReadTable(conn=db, name="HDzzzz")
	
	GetLeadingNumber <- function(x) {
		return(gsub(":.*", "", x))
	}
	
	carnegie_categories <- c("BASIC", "UNDERGRADUATE.INSTRUCTIONAL.PROGRAM", "\\.GRADUATE.INSTRUCTIONAL.PROGRAM", "UNDERGRADUATE.PROFILE", "ENROLLMENT.PROFILE", "SIZE.AND.SETTING") # Need the backslashes to escape the period for Graduate, otherwise it matches both undergraduate and graduate
	
	for (carnegie_category in carnegie_categories) {
		#print(carnegie_category)
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
	print("InstitutionalDirectory")

	# institutional offerings
	ICzzzz <- dbReadTable(conn=db, name="ICzzzz")
	dbWriteTable(conn=dbs, name="InstitutionalOfferings", value=ICzzzz, overwrite=TRUE, append=FALSE)
	rm(ICzzzz)
	print("InstitutionalOfferings")
	
	# institutional charges
	ICzzzz_AY <- dbReadTable(conn=db, name="ICzzzz_AY")
	ICzzzz_AY_selected <- ICzzzz_AY |> dplyr::select("UNITID", "REPORTYEAR", "TABLENAME", dplyr::contains("TUITION"))
	dbWriteTable(conn=dbs, name="InstitutionalCharges", value=ICzzzz_AY_selected, overwrite=TRUE, append=FALSE)
	rm(ICzzzz_AY)
	rm(ICzzzz_AY_selected)
	print("InstitutionalCharges")
	
	# twelve month headcount
	EFFYzzzz <- dbReadTable(conn=db, name="EFFYzzzz")
	dbWriteTable(conn=dbs, name="TwelveMonthHeadcount", value=EFFYzzzz, overwrite=TRUE, append=FALSE)
	rm(EFFYzzzz)
	print("TwelveMonthHeadcount")
	
	# admissions
	ADMzzzz <- dbReadTable(conn=db, name="ADMzzzz")
	dbWriteTable(conn=dbs, name="Admissions", value=ADMzzzz, overwrite=TRUE, append=FALSE)
	rm(ADMzzzz)
	print("Admissions")
	
	# enrollment race
	EFzzzzA <- dbReadTable(conn=db, name="EFzzzzA")
	dbWriteTable(conn=dbs, name="EnrollmentRace", value=EFzzzzA, overwrite=TRUE, append=FALSE)
	rm(EFzzzzA)
	print("EnrollmentRace")
	
	# enrollment major
	EFzzzzCP <- dbReadTable(conn=db, name="EFzzzzCP")
	dbWriteTable(conn=dbs, name="EnrollmentMajor", value=EFzzzzCP, overwrite=TRUE, append=FALSE)
	rm(EFzzzzCP)
	print("EnrollmentMajor")
	
	# enrollment age
	EFzzzzB <- dbReadTable(conn=db, name="EFzzzzB")
	dbWriteTable(conn=dbs, name="EnrollmentAge", value=EFzzzzB, overwrite=TRUE, append=FALSE)
	rm(EFzzzzB)
	print("EnrollmentAge")
	
	# enrollment residence
	EFzzzzC <- dbReadTable(conn=db, name="EFzzzzC")
	dbWriteTable(conn=dbs, name="EnrollmentResidence", value=EFzzzzC, overwrite=TRUE, append=FALSE)
	rm(EFzzzzC)
	print("EnrollmentResidence")
	
	# completions by race
	Czzzz_B <- dbReadTable(conn=db, name="Czzzz_B")
	colnames(Czzzz_B) <- gsub("\\.$", "", gsub("\\.$", "", (colnames(Czzzz_B))))
	dbWriteTable(conn=dbs, name="CompletionsRace", value=Czzzz_B, overwrite=TRUE, append=FALSE)
	rm(Czzzz_B)
	print("CompletionsRace")
	
	# completions by age
	Czzzz_C <- dbReadTable(conn=db, name="Czzzz_C")
	colnames(Czzzz_C) <- gsub("\\.$", "", gsub("\\.$", "", (colnames(Czzzz_C))))
	dbWriteTable(conn=dbs, name="CompletionsAge", value=Czzzz_C, overwrite=TRUE, append=FALSE)
	rm(Czzzz_C)
	print("CompletionsAge")
	
	# instructional gender
	SALzzzz_IS <- dbReadTable(conn=db, name="SALzzzz_IS")
	dbWriteTable(conn=dbs, name="InstructionalGender", value=SALzzzz_IS, overwrite=TRUE, append=FALSE)
	rm(SALzzzz_IS)
	print("InstructionalGender")
	
	# non-instructional staff
	SALzzzz_NIS <- dbReadTable(conn=db, name="SALzzzz_NIS")
	dbWriteTable(conn=dbs, name="NonInstructionalStaff", value=SALzzzz_NIS, overwrite=TRUE, append=FALSE)
	rm(SALzzzz_NIS)
	print("NonInstructionalStaff")
	
	# staff category
	Szzzz_OC <- dbReadTable(conn=db, name="Szzzz_OC")
	dbWriteTable(conn=dbs, name="StaffCategory", value=Szzzz_OC, overwrite=TRUE, append=FALSE)
	rm(Szzzz_OC)
	print("StaffCategory")
	
	#staff tenure category
	Szzzz_SIS <- dbReadTable(conn=db, name="Szzzz_SIS")
	dbWriteTable(conn=dbs, name="StaffTenureCategory", value=Szzzz_SIS, overwrite=TRUE, append=FALSE)
	rm(Szzzz_SIS)
	print("StaffTenureCategory")
	
	# staff gender
	Szzzz_IS <- dbReadTable(conn=db, name="Szzzz_IS")
	dbWriteTable(conn=dbs, name="StaffGender", value=Szzzz_IS, overwrite=TRUE, append=FALSE)
	rm(Szzzz_IS)
	print("StaffGender")
	
	# staff new
	Szzzz_NH <- dbReadTable(conn=db, name="Szzzz_NH")
	dbWriteTable(conn=dbs, name="StaffNew", value=Szzzz_NH, overwrite=TRUE, append=FALSE)
	rm(Szzzz_NH)
	print("StaffNew")
	
	# employees position
	EAPzzzz <- dbReadTable(conn=db, name="EAPzzzz")
	dbWriteTable(conn=dbs, name="EmployeesPosition", value=EAPzzzz, overwrite=TRUE, append=FALSE)
	rm(EAPzzzz)
	print("EmployeesPosition")
	
	# student aid overview
	SFAzzzz_P1 <- dbReadTable(conn=db, name="SFAzzzz_P1")
	dbWriteTable(conn=dbs, name="StudentAidOverview", value=SFAzzzz_P1, overwrite=TRUE, append=FALSE)
	rm(SFAzzzz_P1)
	print("StudentAidOverview")

	# yearly results
	SFAzzzz_P2 <- dbReadTable(conn=db, name="SFAzzzz_P2")
	
	# The raw table gets too wide, so we move the below processing code earlier in the run.
	
	# financial_aid_by_year <- data.frame()
	# year_data_indices <- which(grepl("20\\d\\d\\.\\d\\d$", colnames(SFAzzzz_P2)))
	# for (year_data_index in year_data_indices) {
	# 	full_title <- strsplit(colnames(SFAzzzz_P2)[year_data_index], "___")[[1]][2]
	# 	actual_year <- as.numeric(substr(substr(full_title, nchar(full_title)-6, nchar(full_title)),1,4))
	# 	actual_title <- gsub("\\.$", "", substr(full_title, 1, nchar(full_title)-9))
	# 	local_df <- data.frame(UNITID=SFAzzzz_P2$UNITID, REPORTYEAR=SFAzzzz_P2$REPORTYEAR, ACTUALYEAR=actual_year, TABLENAME=SFAzzzz_P2$TABLENAME, COLUMN=actual_title, VALUE=SFAzzzz_P2[[year_data_index]])
	# 	local_df <- local_df[!is.na(local_df$VALUE),]
	# 	local_df <- local_df[local_df$REPORTYEAR-local_df$ACTUALYEAR<2,]
	# 	financial_aid_by_year <- dplyr::bind_rows(financial_aid_by_year, local_df)
	# 	#print(nrow(financial_aid_by_year))
	# 	#print(year_data_index)
	# }
	dbWriteTable(conn=dbs, name="StudentAidYearly", value=SFAzzzz_P2, overwrite=TRUE, append=FALSE)
	rm(SFAzzzz_P2)
	print("StudentAidYearly")
	
	# graduation data four years
	GRzzzz <- dbReadTable(conn=db, name="GRzzzz")
	
	
	dbWriteTable(conn=dbs, name="GraduationFourYear", value=GRzzzz, overwrite=TRUE, append=FALSE)
	rm(GRzzzz)
	print("GraduationFourYear")
	
	# graduation data L2 
	GRzzzz_L2 <- dbReadTable(conn=db, name="GRzzzz_L2")
	dbWriteTable(conn=dbs, name="GraduationTwoYear", value=GRzzzz_L2, overwrite=TRUE, append=FALSE)
	rm(GRzzzz_L2)
	print("GraduationTwoYear")
	
	# graduation data PELL
	GRzzzz_PELL_SSL <- dbReadTable(conn=db, name="GRzzzz_PELL_SSL")
	dbWriteTable(conn=dbs, name="GraduationPELL", value=GRzzzz_PELL_SSL, overwrite=TRUE, append=FALSE)
	rm(GRzzzz_PELL_SSL)
	print("GraduationPELL")
	
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
	print("Library")

	# comparison table 
	CUSTOMCGIDSzzzz <- dbReadTable(conn=db, name="CUSTOMCGIDSzzzz")
	dbWriteTable(conn=dbs, name="ComparisonInstitutionTable", value=CUSTOMCGIDSzzzz, overwrite=TRUE, append=FALSE)
	rm(CUSTOMCGIDSzzzz)
	print("ComparisonInstitutionTable")

	return("data/db_sensible.sqlite")
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
	ComparisonInputGrouped <- ComparisonInput |> group_by(from) |> summarise(to=paste(to, collapse=", "), n=n())
	ComparisonOutputGrouped <- ComparisonInput |> group_by(to) |> summarise(from=paste(from, collapse=", "), n=n())
	ComparisonBoth <- data.frame(focal=unique(c(ComparisonInputGrouped$from, ComparisonOutputGrouped$to)))
	ComparisonBoth$to_all <- NA
	ComparisonBoth$from_all <- NA
	ComparisonBoth$to_only <- NA
	ComparisonBoth$from_only <- NA
	ComparisonBoth$mutuals <- NA
	for (i in sequence(nrow(ComparisonBoth))) {
		ComparisonBoth$to_all[i] <- ComparisonInputGrouped$to[match(ComparisonBoth$focal[i], ComparisonInputGrouped$from)]
		to_vector <- strsplit(ComparisonBoth$to_all[i], ", ")[[1]]
		ComparisonBoth$from_all[i] <- ComparisonOutputGrouped$from[match(ComparisonBoth$focal[i], ComparisonOutputGrouped$to)]
		from_vector <- strsplit(ComparisonBoth$from_all[i], ", ")[[1]]
		ComparisonBoth$to_only[i] <- paste0(setdiff(to_vector, from_vector), collapse=", ")
		ComparisonBoth$from_only[i] <- paste0(setdiff(from_vector, to_vector), collapse=", ")
		ComparisonBoth$mutuals[i] <- paste0(intersect(to_vector, from_vector), collapse=", ")
	}
	
	
	
	

	return(Sys.time())
}


CreateCollegeDataAllYears <- function(placeholder) {
	dbs <- dbConnect(RSQLite::SQLite(), "data/db_sensible.sqlite")
	InstitutionalDirectory <- dbReadTable(conn=dbs, name="InstitutionalDirectory") |> dplyr::select(c("UNITID", "INSTNM___INSTITUTION..ENTITY..NAME", "ADDR___STREET.ADDRESS.OR.POST.OFFICE.BOX", 
"CITY___CITY.LOCATION.OF.INSTITUTION", "STABBR___STATE.ABBREVIATION", 
"ZIP___ZIP.CODE", "FIPS___FIPS.STATE.CODE", "OBEREG___GEOGRAPHIC.REGION", 
"CHFNM___NAME.OF.CHIEF.ADMINISTRATOR", "CHFTITLE___TITLE.OF.CHIEF.ADMINISTRATOR", 
"OPEID___OFFICE.OF.POSTSECONDARY.EDUCATION..OPE..ID.NUMBER", 
"OPEFLAG___OPE.TITLE.IV.ELIGIBILITY.INDICATOR.CODE", "WEBADDR___INSTITUTION.S.INTERNET.WEBSITE.ADDRESS", 
"ADMINURL___ADMISSIONS.OFFICE.WEB.ADDRESS", "FAIDURL___FINANCIAL.AID.OFFICE.WEB.ADDRESS", 
"APPLURL___ONLINE.APPLICATION.WEB.ADDRESS", "NPRICURL___NET.PRICE.CALCULATOR.WEB.ADDRESS", 
"SECTOR___SECTOR.OF.INSTITUTION", "ICLEVEL___LEVEL.OF.INSTITUTION", 
"CONTROL___CONTROL.OF.INSTITUTION", "HLOFFER___HIGHEST.LEVEL.OF.OFFERING", 
"UGOFFER___UNDERGRADUATE.OFFERING", "GROFFER___GRADUATE.OFFERING", 
"HDEGOFR1___HIGHEST.DEGREE.OFFERED", "DEGGRANT___DEGREE.GRANTING.STATUS", 
"HBCU___HISTORICALLY.BLACK.COLLEGE.OR.UNIVERSITY", "HOSPITAL___INSTITUTION.HAS.HOSPITAL", 
"MEDICAL___INSTITUTION.GRANTS.A.MEDICAL.DEGREE", "TRIBAL___TRIBAL.COLLEGE", 
"LOCALE___DEGREE.OF.URBANIZATION..URBAN.CENTRIC.LOCALE", "OPENPUBL___INSTITUTION.OPEN.TO.THE.GENERAL.PUBLIC", 
"ACT___STATUS.OF.INSTITUTION", "NEWID___UNITID.FOR.MERGED.SCHOOLS", 
"DEATHYR___YEAR.INSTITUTION.WAS.DELETED.FROM.IPEDS", "CLOSEDAT___DATE.INSTITUTION.CLOSED", 
"CYACTIVE___INSTITUTION.IS.ACTIVE.IN.CURRENT.YEAR", "POSTSEC___PRIMARILY.POSTSECONDARY.INDICATOR", 
"PSEFLAG___POSTSECONDARY.INSTITUTION.INDICATOR", "PSET4FLG___POSTSECONDARY.AND.TITLE.IV.INSTITUTION.INDICATOR", 
"RPTMTH___REPORTING.METHOD.FOR.STUDENT.CHARGES..GRADUATION.RATES..RETENTION.RATES.AND.STUDENT.FINANCIAL.AID", 
"IALIAS___INSTITUTION.NAME.ALIAS", "INSTCAT___INSTITUTIONAL.CATEGORY",  "LANDGRNT___LAND.GRANT.INSTITUTION", 
"INSTSIZE___INSTITUTION.SIZE.CATEGORY", "CBSA___CORE.BASED.STATISTICAL.AREA..CBSA", 
"CBSATYPE___CBSA.TYPE.METROPOLITAN.OR.MICROPOLITAN", "CSA___COMBINED.STATISTICAL.AREA..CSA", 
"F1SYSTYP___SYSTEM..GOVERNING.BOARD.OR.CORPORATE.STRUCTURE", 
"F1SYSNAM___NAME.OF.SYSTEM..GOVERNING.BOARD.OR.CORPORATE.ENTITY", 
"COUNTYCD___FIPS.COUNTY.CODE", "COUNTYNM___COUNTY.NAME",  "LONGITUD___LONGITUDE.LOCATION.OF.INSTITUTION", 
"LATITUDE___LATITUDE.LOCATION.OF.INSTITUTION", 
"REPORTYEAR","F1SYSTYP___MULTI.INSTITUTION.OR.MULTI.CAMPUS.ORGANIZATION", 
"F1SYSNAM___NAME.OF.MULTI.INSTITUTION.OR.MULTI.CAMPUS.ORGANIZATION", 
"F1SYSCOD___IDENTIFICATION.NUMBER.OF.MULTI.INSTITUTION.OR.MULTI.CAMPUS.ORGANIZATION", 
"OBEREG___BUREAU.OF.ECONOMIC.ANALYSIS..BEA..REGIONS", "VETURL___VETERANS.AND.MILITARY.SERVICEMEMBERS.TUITION.POLICIES.WEB.ADDRESS", 
"ATHURL___STUDENT.RIGHT.TO.KNOW.STUDENT.ATHLETE.GRADUATION.RATE.WEB.ADDRESS", 
"DISAURL___DISABILITY.SERVICES.WEB.ADDRESS", 
"UEIS___UNIQUE.ENTITY.IDENTIFIER..UEI..NUMBERS", "CARNEGIE.BASIC", 
"CARNEGIE.UNDERGRADUATE.INSTRUCTIONAL.PROGRAM", "CARNEGIE.GRADUATE.INSTRUCTIONAL.PROGRAM", 
"CARNEGIE.UNDERGRADUATE.PROFILE", "CARNEGIE.ENROLLMENT.PROFILE", 
"CARNEGIE.SIZE.AND.SETTING"))
	college_data <- tidyr::pivot_longer(InstitutionalDirectory, cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	college_data$Category <- "InstitutionalDirectory"
	rm(InstitutionalDirectory)
	print(table(college_data$Category))
	
	InstitutionalOfferings <- dbReadTable(conn=dbs, name="InstitutionalOfferings") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	InstitutionalOfferings$Category <- "InstitutionalOfferings"
	college_data <- dplyr::bind_rows(college_data, InstitutionalOfferings)
	rm(InstitutionalOfferings)
	print(table(college_data$Category))

	
	InstitutionalCharges <- dbReadTable(conn=dbs, name="InstitutionalCharges") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	InstitutionalCharges$Category <- "InstitutionalCharges"
	college_data <- dplyr::bind_rows(college_data, InstitutionalCharges)
	rm(InstitutionalCharges)
	print(table(college_data$Category))

	
	TwelveMonthHeadcount <- dbReadTable(conn=dbs, name="TwelveMonthHeadcount") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	TwelveMonthHeadcount$Category <- "TwelveMonthHeadcount"
	college_data <- dplyr::bind_rows(college_data, TwelveMonthHeadcount)
	rm(TwelveMonthHeadcount)
	print(table(college_data$Category))
	
	Admissions <- dbReadTable(conn=dbs, name="Admissions") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	Admissions$Category <- "Admissions"
	college_data <- dplyr::bind_rows(college_data, Admissions)
	rm(Admissions)
	print(table(college_data$Category))
	
	EnrollmentRace <- dbReadTable(conn=dbs, name="EnrollmentRace") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	EnrollmentRace$Category <- "EnrollmentRace"
	college_data <- dplyr::bind_rows(college_data, EnrollmentRace)
	rm(EnrollmentRace)
	print(table(college_data$Category))
	
	EnrollmentMajor <- dbReadTable(conn=dbs, name="EnrollmentMajor") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	EnrollmentMajor$Category <- "EnrollmentMajor"
	college_data <- dplyr::bind_rows(college_data, EnrollmentMajor)
	rm(EnrollmentMajor)
	print(table(college_data$Category))
	
	EnrollmentAge <- dbReadTable(conn=dbs, name="EnrollmentAge") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	EnrollmentAge$Category <- "EnrollmentAge"
	college_data <- dplyr::bind_rows(college_data, EnrollmentAge)
	rm(EnrollmentAge)
	print(table(college_data$Category))
	
	EnrollmentResidence <- dbReadTable(conn=dbs, name="EnrollmentResidence") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	EnrollmentResidence$Category <- "EnrollmentResidence"
	college_data <- dplyr::bind_rows(college_data, EnrollmentResidence)
	rm(EnrollmentResidence)
	print(table(college_data$Category))
	
	CompletionsRace <- dbReadTable(conn=dbs, name="CompletionsRace") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	CompletionsRace$Category <- "CompletionsRace"
	college_data <- dplyr::bind_rows(college_data, CompletionsRace)
	rm(CompletionsRace)
	print(table(college_data$Category))
	
	CompletionsAge <- dbReadTable(conn=dbs, name="CompletionsAge") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	CompletionsAge$Category <- "CompletionsAge"
	college_data <- dplyr::bind_rows(college_data, CompletionsAge)
	rm(CompletionsAge)
	print(table(college_data$Category))
	
	InstructionalGender <- dbReadTable(conn=dbs, name="InstructionalGender") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	InstructionalGender$Category <- "InstructionalGender"
	college_data <- dplyr::bind_rows(college_data, InstructionalGender)
	rm(InstructionalGender)
	print(table(college_data$Category))
	
	NonInstructionalStaff <- dbReadTable(conn=dbs, name="NonInstructionalStaff") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	NonInstructionalStaff$Category <- "NonInstructionalStaff"
	college_data <- dplyr::bind_rows(college_data, NonInstructionalStaff)
	rm(NonInstructionalStaff)
	print(table(college_data$Category))
	
	StaffCategory <- dbReadTable(conn=dbs, name="StaffCategory") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	StaffCategory$Category <- "StaffCategory"
	college_data <- dplyr::bind_rows(college_data, StaffCategory)
	rm(StaffCategory)
	print(table(college_data$Category))
	
	StaffTenureCategory <- dbReadTable(conn=dbs, name="StaffTenureCategory") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	StaffTenureCategory$Category <- "StaffTenureCategory"
	college_data <- dplyr::bind_rows(college_data, StaffTenureCategory)
	rm(StaffTenureCategory)
	print(table(college_data$Category))
	
	StaffGender <- dbReadTable(conn=dbs, name="StaffGender") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	StaffGender$Category <- "StaffGender"
	college_data <- dplyr::bind_rows(college_data, StaffGender)
	rm(StaffGender)
	print(table(college_data$Category))
	
	StaffNew <- dbReadTable(conn=dbs, name="StaffNew") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	StaffNew$Category <- "StaffNew"
	college_data <- dplyr::bind_rows(college_data, StaffNew)
	rm(StaffNew)
	print(table(college_data$Category))
	
	EmployeesPosition <- dbReadTable(conn=dbs, name="EmployeesPosition") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	EmployeesPosition$Category <- "EmployeesPosition"
	college_data <- dplyr::bind_rows(college_data, EmployeesPosition)
	rm(EmployeesPosition)
	print(table(college_data$Category))
	
	StudentAidOverview <- dbReadTable(conn=dbs, name="StudentAidOverview") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	StudentAidOverview$Category <- "StudentAidOverview"
	college_data <- dplyr::bind_rows(college_data, StudentAidOverview)
	rm(StudentAidOverview)
	print(table(college_data$Category))
	
	# Remember this is weird b/c we pivoted it already
	StudentAidYearly <- dbReadTable(conn=dbs, name="StudentAidYearly") 
	StudentAidYearlyFormatted <- data.frame(UNITID=StudentAidYearly$UNITID, REPORTYEAR=StudentAidYearly$REPORTYEAR, Variable=StudentAidYearly$COLUMN, Value=StudentAidYearly$VALUE, Category="StudentAidYearly")
	college_data <- dplyr::bind_rows(college_data, StudentAidYearlyFormatted)
	rm(StudentAidYearly)
	rm(StudentAidYearlyFormatted)
	print(table(college_data$Category))
	
	GraduationFourYear <- dbReadTable(conn=dbs, name="GraduationFourYear") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	GraduationFourYear$Category <- "GraduationFourYear"
	college_data <- dplyr::bind_rows(college_data, GraduationFourYear)
	rm(GraduationFourYear)
	print(table(college_data$Category))
	
	GraduationTwoYear <- dbReadTable(conn=dbs, name="GraduationTwoYear") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	GraduationTwoYear$Category <- "GraduationTwoYear"
	college_data <- dplyr::bind_rows(college_data, GraduationTwoYear)
	rm(GraduationTwoYear)
	print(table(college_data$Category))

	
	GraduationPELL <- dbReadTable(conn=dbs, name="GraduationPELL") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	GraduationPELL$Category <- "GraduationPELL"
	college_data <- dplyr::bind_rows(college_data, GraduationPELL)
	rm(GraduationPELL)
	print(table(college_data$Category))
	
	Library <- dbReadTable(conn=dbs, name="Library") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	Library$Category <- "Library"
	college_data <- dplyr::bind_rows(college_data, Library)
	rm(Library)
	print(table(college_data$Category))
	
	ComparisonInstitutionTable <- dbReadTable(conn=dbs, name="ComparisonInstitutionTable") |> tidyr::pivot_longer(cols=-c(UNITID, REPORTYEAR), names_to="Variable", values_to="Value", values_drop_na=TRUE)
	ComparisonInstitutionTable$Category <- "ComparisonInstitutionTable"
	college_data <- dplyr::bind_rows(college_data, ComparisonInstitutionTable)
	rm(ComparisonInstitutionTable)
	print(table(college_data$Category))
	
	
	return(college_data)
}

AggregatePercentages <- function(college_data) {
	
	AdmissionRateDataWide <- college_data[college_data$Variable %in% c("UNITID", "REPORTYEAR", "ADMSSN___ADMISSIONS.TOTAL", "ADMSSNM___ADMISSIONS.MEN", "ADMSSNW___ADMISSIONS.WOMEN", "ADMSSNAN___ADMISSIONS.ANOTHER.GENDER", "APPLCN___APPLICANTS.TOTAL", "APPLCNM___APPLICANTS.MEN", "APPLCNAN___APPLICANTS.ANOTHER.GENDER", "APPLCNW___APPLICANTS.WOMEN", "ENRLAN___ENROLLED.ANOTHER.GENDER", "ENRLT___ENROLLED.TOTAL", "ENRLW___ENROLLED..WOMEN", "ENRLM___ENROLLED..MEN"),] |> pivot_wider(id_cols=c("UNITID", "REPORTYEAR"), names_from="Variable", values_from="Value")
	
	for(col_index in 3:ncol(AdmissionRateDataWide)) {
		AdmissionRateDataWide[[col_index]] <- as.numeric(AdmissionRateDataWide[[col_index]])
	}
	
	
	college_data <- rbind(college_data, data.frame(UNITID=AdmissionRateDataWide$UNITID, REPORTYEAR=AdmissionRateDataWide$REPORTYEAR, Variable="ADMISSION.RATE.TOTAL", Value=100*AdmissionRateDataWide$ADMSSN___ADMISSIONS.TOTAL/AdmissionRateDataWide$APPLCN___APPLICANTS.TOTAL, Category="Admissions"))
	
	college_data <- rbind(college_data, data.frame(UNITID=AdmissionRateDataWide$UNITID, REPORTYEAR=AdmissionRateDataWide$REPORTYEAR, Variable="ADMISSION.RATE.MEN", Value=100*AdmissionRateDataWide$ADMSSNM___ADMISSIONS.MEN/AdmissionRateDataWide$APPLCNM___APPLICANTS.MEN, Category="Admissions"))
	
	college_data <- rbind(college_data, data.frame(UNITID=AdmissionRateDataWide$UNITID, REPORTYEAR=AdmissionRateDataWide$REPORTYEAR, Variable="ADMISSION.RATE.WOMEN", Value=100*AdmissionRateDataWide$ADMSSNW___ADMISSIONS.WOMEN/AdmissionRateDataWide$APPLCNW___APPLICANTS.WOMEN, Category="Admissions"))
	
	college_data <- rbind(college_data, data.frame(UNITID=AdmissionRateDataWide$UNITID, REPORTYEAR=AdmissionRateDataWide$REPORTYEAR, Variable="ADMISSION.RATE.ANOTHER.GENDER", Value=100*AdmissionRateDataWide$ADMSSNAN___ADMISSIONS.ANOTHER.GENDER/AdmissionRateDataWide$APPLCNAN___APPLICANTS.ANOTHER.GENDER, Category="Admissions"))
	
	college_data <- rbind(college_data, data.frame(UNITID=AdmissionRateDataWide$UNITID, REPORTYEAR=AdmissionRateDataWide$REPORTYEAR, Variable="YIELD.RATE.TOTAL", Value=100*AdmissionRateDataWide$ENRLT___ENROLLED.TOTAL/AdmissionRateDataWide$ADMSSNM___ADMISSIONS.MEN, Category="Admissions"))
	
	college_data <- rbind(college_data, data.frame(UNITID=AdmissionRateDataWide$UNITID, REPORTYEAR=AdmissionRateDataWide$REPORTYEAR, Variable="YIELD.RATE.WOMEN", Value=100*AdmissionRateDataWide$ENRLW___ENROLLED..WOMEN/AdmissionRateDataWide$ADMSSNW___ADMISSIONS.WOMEN, Category="Admissions"))
	
	college_data <- rbind(college_data, data.frame(UNITID=AdmissionRateDataWide$UNITID, REPORTYEAR=AdmissionRateDataWide$REPORTYEAR, Variable="YIELD.RATE.ANOTHER.GENDER", Value=100*AdmissionRateDataWide$ENRLAN___ENROLLED.ANOTHER.GENDER/AdmissionRateDataWide$ADMSSNAN___ADMISSIONS.ANOTHER.GENDER, Category="Admissions"))
	
	rm(AdmissionRateDataWide)
	
	
	
	return(college_data)	
}

GetLatestYear <- function(college_data) {
	college_data$IndexColumn <- paste0(college_data$UNITID, "___", college_data$Variable)	
	latest_year <- college_data[order(college_data$REPORTYEAR, decreasing=TRUE),]
	latest_year <- latest_year[!duplicated(latest_year$IndexColumn),]
	latest_year <- latest_year |> dplyr::select(-IndexColumn)
	return(latest_year)
}