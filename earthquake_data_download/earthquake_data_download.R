library(tidyr)
library(dplyr)
library(readr)
library(stringr)
library(lubridate)
library(httr)


###############################################################################
# 1. DOWNLOAD DIRECTORY CHECK/CREATE & SET
###############################################################################

# Time the running of the whole script. Start time.
start_time <- proc.time()

# Set working directory. 
setwd(getSrcDirectory(function(){})[1])

# Set file path. 
file_path <- "./EQData"

# Check if the folder exists, if not, create one.
if (!dir.exists(file_path)) {
  dir.create(file_path)
}


###############################################################################
# 2. DOWNLOADING FILES
###############################################################################

ValidateDates <- function(startDate, endDate) {
  # Validates the start and end date range arguments and shows stop message if 
  # one of the validations failed. 
  # Validation 1: Check if the given arguments are not valid dates.
  # Validation 2: Check if the End Date is earlier than the Start Date.
  # 
  # Args: 
  #    startDate: Start Date parameter of the query. 
  #    endDate: End Date parameter of the query. Both dates must be date data 
  #             type.
  #
  # Returns: 
  #    No returns. 

  # Check if the given arguments are not valid dates.
  if (!is.Date(startDate) | !is.Date(endDate)) {
    stop("Please give valid date parameters using as.Date or as_date 
         functions.")
  }
  # Check if the End Date is earlier than the Start Date.
  else if (endDate < startDate) {
    stop("End Date must be later than the Start Date.")
  }
}


CheckRecordsCount <- function(startDate, endDate) {
  # Queries the record count to be downloaded for the given date arguments. 
  # 
  # Args: 
  #    startDate: Start Date parameter of the query. 
  #    endDate: End Date parameter of the query. Both dates must be date data 
  #             type.
  #
  # Returns: 
  #    The total number of records. Integer data type. 
  
  # Validate date arguments.
  ValidateDates(startDate, endDate)

  records_num_url <-
    GET(
      paste0(
        "https://earthquake.usgs.gov/fdsnws/event/1/count?starttime=",
        startDate,
        "&endtime=",
        endDate,
        "23:59:59",
        "&minmagnitude=1"
      )
    )
  total_records <- as.integer(content(records_num_url))
  return(total_records)
}


Download <- function(startDate, endDate, fileName) {
  # Downloads the query results as a csv file using the query URL. 
  # 
  # Args: 
  #    startDate: Start Date parameter of the query. 
  #    endDate: End Date parameter of the query. Both dates must be date data 
  #             type.
  #    fileName: File name to be saved as, including path and extension (.csv).
  #              Must be string data type.
  #
  # Returns: 
  #    The total number of records. Integer data type. 
  
  # Validate date arguments.
  ValidateDates(startDate, endDate)

  file_url <-
    paste0(
      "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=",
      startDate,
      "&endtime=",
      endDate,
      "23:59:59",
      "&minmagnitude=1"
    )
  download.file(file_url, destfile = fileName, method = "curl")
}


DownloadFiles <- function(startDate, endDate) {
  # Prepares to download csv files per month using the given date arguments. 
  # 
  # Args: 
  #    startDate: Start Date parameter of the query. 
  #    endDate: End Date parameter of the query. Both dates must be date data 
  #             type.
  #
  # Returns: 
  #    A success message. String data type. 
  
  # Validate date parameters.
  ValidateDates(startDate, endDate)

  # define count of records to download for each iteration
  records_to_dl <- 0

  # define count of all downloaded records
  downloaded_records <- 0

  # define iteration count
  itr_count <- 0

  # define iteration stop flag
  last_dl <- FALSE

  # For the file download, the result records limit is 20,000 per download.
  # So, for easier tracking, the file is downloaded per month using iteration.
  # If per month still exceeds 20,000 records, the first 15 days is downloaded 
  # in one file and the remaining days in another file.
  while (last_dl == FALSE) {  # iterate until the flag is set as TRUE.
    # The logic for setting curr_start_date is to check the iteration count.
    # If it's the 1st iteration, the given startDate argument is used. 
    # If it's not the 1st iteration, the value is set by adding one day to the
    # previous iteration's End Date, curr_end_date.
    if (itr_count < 1) {
      curr_start_date <- startDate
    } else {
      curr_start_date <- curr_end_date + days(1)
    }

    # curr_end_date is set as the last day of the month from curr_start_date 
    # and validate against the given endDate argument to check if it's later 
    # than the endDate. If it is, the curr_end_date is set with the value of 
    # endDate argument.
    curr_end_date <- ceiling_date(curr_start_date, "month") %m-% days(1)

    if (curr_end_date >= endDate) {
      
      curr_end_date <- endDate  # Set as the given argument.
      last_dl <- TRUE  # Set the flag as TRUE to end the iteration.
    }

    # Check the current record counts to be downloaded.
    records_to_dl <-
      CheckRecordsCount(curr_start_date, curr_end_date)

    # Check if the record limit is exceeded, if true, separate as two files.
    # If not, download the records.
    if (records_to_dl > 20000) {
      # Set file name of the 1st file to be saved as, including path and 
      # extension .csv. 
      file_name <-
        paste0(
          file_path,
          "/earthquake_data_",
          year(curr_start_date),
          "_",
          month(curr_start_date, label = TRUE),
          "-1.csv"
        )
      
      # Download the first 15 days.
      Download(
        curr_start_date,
        curr_start_date + days(14),
        file_name
      )
      
      # Set file name of the 2nd file to be saved as, including path and 
      # extension .csv. 
      file_name <-
        paste0(
          file_path,
          "/earthquake_data_",
          year(curr_start_date),
          "_",
          month(curr_start_date, label = TRUE),
          "-2.csv"
        )
      
      # Download remaining days.
      Download(curr_start_date + days(15), curr_end_date, file_name)
    } else {
      # Set file name to be saved as, including path and extension .csv. 
      file_name <-
        paste0(
          file_path,
          "/earthquake_data_",
          year(curr_start_date),
          "_",
          month(curr_start_date, label = TRUE),
          ".csv"
        )
      
      # Download the file for the month.
      Download(curr_start_date, curr_end_date, file_name)
    }
    # Increment the downloaded records.
    downloaded_records <- downloaded_records + records_to_dl
    # Increment the iteration count.
    itr_count <- itr_count + 1
  }
  
  # Return the success message. 
  return(
    paste0(
      "Successfully downloaded ",
      downloaded_records,
      " records in ",
      itr_count,
      " files."
    )
  )
}


###############################################################################
# 3. MERGING DATA FROM DOWNLOADED FILES
###############################################################################

MergeFiles <- function() {
  # Reads all the files in the given directory and bind them by rows. 
  # 
  # Args: 
  #    None.
  #
  # Returns: 
  #    A data frame of merged rows. 
  
  eqk_data <- list.files(path = file_path, full.names = TRUE) %>%
    lapply(read_csv) %>%
    bind_rows()
  
  return(eqk_data)
}


###############################################################################
# 4. CLEANING MERGED DATA
###############################################################################

RemoveDuplicates <- function(df) {
  # Removes all the duplicated rows. 
  # 
  # Args: 
  #    df: A data frame to remove duplicates.
  #
  # Returns: 
  #    A data frame of unique rows. 
  
  # Check if duplicated rows exists
  duplicate_rows <- df %>%
    group_by_all() %>%
    filter(n() > 1) %>%
    ungroup()
  
  # Remove duplicates if there is any
  if (nrow(duplicate_rows) > 0) {
    df <- distinct(df)
  }
  
  return(df)
}


CheckMissingDates <- function(df, startDate, endDate) {
  # Prints all the missing dates from the data frame between the start date and
  # the end date. If there is a missing date, it is most likely that the
  # downloaded files were incomplete.  
  # 
  # Args: 
  #    df: A data frame to check the missing dates.
  #    startDate: Start Date parameter to check the missing dates. 
  #    endDate: End Date parameter to check the missing dates. Both dates must
  #             be date data type.
  #
  # Returns: 
  #    A message with the missing dates (if any). 
  
  # Convert time column as Date data type.
  df$time <- as.Date(df$time)
  
  # Get a list of all dates within the given start and end dates in sequence.
  all_dates <- seq(startDate, endDate, by = 1)
  
  # Check if there is any date from all_dates that are not in df.
  missing_dates <- all_dates[!all_dates %in% df$time]
  
  # Set return message.
  if (length(missing_dates) > 0) {
    return_message <- paste0("Missing data for following dates: ",
                             missing_dates)
  } else {
    return_message <- "There is no missing date."
  }
  
  return(return_message)
}


SeparateLocationColumn <- function(df) {
  # Separates 'place' column into two columns: 'Location Detail' and 'Location'.
  # The last comma is used as a separator.
  # 
  # Args: 
  #    df: A data frame to check the missing dates.
  #
  # Returns: 
  #    A data frame with separated columns. 
  
  df_separated <-
    separate(
      data = df,
      col = "place",
      into = c("Location Detail", "Location"),
      sep = ",(?=[^,]*$)"
    )
  
  return(df_separated)
}


CleanData <- function(df) {
  # Cleans the Location column by trimming extra spaces and setting title case.
  # Drops the records with NA in location, latitude, longitude columns. 
  # Replaces the abbreviations. 
  # 
  # Args: 
  #    df: A data frame to clean the data.
  #
  # Returns: 
  #    A data frame with a cleaned data. 
  
  df_cleaned <- df %>%
    mutate(
      Location = case_when(is.na(Location) ~ `Location Detail`, 
                           TRUE ~ Location) %>%
        str_trim(.) %>%
        str_to_title(.) %>%
        str_replace(., "Ak", "Alaska") %>%
        str_replace(., "Ca", "California") %>%
        str_replace(., "Hi", "Hawaii") %>%
        str_replace(., "Mo", "Missouri") %>%
        str_replace(., "Mt", "Montana") %>%
        str_replace(., "Mx", "Mexico") %>%
        str_replace(., "Nv", "Nevada") %>%
        str_replace(., "Wa", "Washington")
    ) %>%
    drop_na(latitude) %>% # Drop records with latitude as NA
    drop_na(longitude) %>% # Drop records with longitude as NA
    drop_na(Location) # Drop records with Location as NA
  
  # Get all locations with abbreviations. 
  # df_abbr <- df_cleaned$Location[nchar(as.character(df_cleaned$Location)) < 4]
  
  # Check the unique abbreviations.
  # sort(unique(df_abbr))
  
  return(df_cleaned)
}


###############################################################################
# 5. CREATE A FILE WITH CLEANED DATA
###############################################################################

CreateCSV <- function(df) {
  # Creates a csv file with the merged and cleaned data.
  # 
  # Args: 
  #    df: A data frame of cleaned data.
  #
  # Returns: 
  #    Success message. 
  
  write.csv(df, file = "./earthquakes-data.csv")
  return("File created with name earthquakes-data.csv")
}


###############################################################################
# 6. CALLING FUNCTIONS
###############################################################################

# Set start date and end date of the search query. Use yyyy-mm-dd date format.
start_date <- as_date("2013-01-01")
end_date <- Sys.Date()

# Use system.time function to record the time taken to run each function.
# system.time(total_record <- {
#   CheckRecordsCount(start_date, end_date)
# })
# system.time(download_message <- {
#   DownloadFiles(start_date, end_date)
# })
# system.time(df_merged <- {
#   MergeFiles()
# })
# system.time(df_unique <- {
#   RemoveDuplicates(df_merged) 
# })
# system.time(missing_dates <- {
#   CheckMissingDates(df_unique, start_date, end_date) 
# })
# system.time(df_separated <- {
#   SeparateLocationColumn(df_unique)
# })
# system.time(df_cleaned <- {
#   CleanData(df_separated)
# })
# system.time(create_message <- { 
#   CreateCSV(df_cleaned)
# })

# Run the functions without recording time for each one.
total_record <- CheckRecordsCount(start_date, end_date)
download_message <- DownloadFiles(start_date, end_date)
df_merged <- MergeFiles()
df_unique <- RemoveDuplicates(df_merged)
message <- CheckMissingDates(df_merged, start_date, end_date)
df_separated <- SeparateLocationColumn(df_unique)
df_cleaned <- CleanData(df_separated)
create_message <- CreateCSV(df_cleaned)

print(download_message)
print(create_message)

# Time the running of the whole script. End time.
print(proc.time() - start_time)
