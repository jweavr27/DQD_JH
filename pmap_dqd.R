# =============================================================================
# DataQualityDashboard on Databricks OMOP Database
# =============================================================================

# -----------------------------------------------------------------------------
# 1. Install and Load Required Libraries
# -----------------------------------------------------------------------------

# Install packages if not already installed
install_if_missing <- function(packages) {
  for (pkg in packages) {
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      if (pkg %in% c("DataQualityDashboard", "DatabaseConnector")) {
        # Install from OHDSI GitHub
        remotes::install_github(paste0("OHDSI/", pkg))
      } else {
        install.packages(pkg)
      }
    }
  }
}

# Required packages
required_packages <- c(
  "remotes",
  "DatabaseConnector",
  "DataQualityDashboard",
  "dplyr",
  "jsonlite"
)

install_if_missing(required_packages)

# Load libraries
library(DatabaseConnector)
library(DataQualityDashboard)
library(dplyr)
library(jsonlite)

# -----------------------------------------------------------------------------
# 2. Configuration Settings
# -----------------------------------------------------------------------------
#downloadJdbcDrivers("spark",pathToDriver = 'C:/Users/jweave44/OHDSI_drivers')
# Databricks connection settings
databricks_config <- list(
  # JDBC Driver location
  #jdbc_driver_path = "C:/Users/jweave44/DatabricksJDBC42-2.6.36.1062",
  jdbc_driver_path = "C:/Users/jweave44/DatabricksJDBC-3.3.1",
  #jdbc_driver_path = "C:/Users/jweave44/OHDSI_drivers",
  
  # Databricks workspace URL (modify as needed)
  # Format: jdbc:databricks://<server-hostname>:443/default;transportMode=http;ssl=1;httpPath=<http-path>;AuthMech=3;
  server_hostname = "adb-8642801621716302.2.azuredatabricks.net",  # MODIFY THIS
  http_path = "/sql/1.0/warehouses/c9f3034f2da6a31c",  # MODIFY THIS
  
  # Database/Schema settings for OMOP CDM
  cdm_database_schema = "data_mgmt.jhm_omop",  # MODIFY THIS - Schema containing OMOP CDM tables
  results_database_schema = "clinical_notes.nlp_dobbins",  # MODIFY THIS - Schema to write results
  vocab_database_schema = "data_mgmt.jhm_omop",  # MODIFY THIS - Usually same as CDM schema
  
  # CDM Source Name
  cdm_source_name = "Johns Hopkins Medical Enterprise"  # MODIFY THIS
)

# DataQualityDashboard output settings
output_config <- list(
  output_folder = "./DQD_Results",
  output_file = paste0("DQD_Results_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".json")
)

# -----------------------------------------------------------------------------
# 3. Retrieve Databricks Personal Access Token
# -----------------------------------------------------------------------------

get_databricks_token <- function() {
  token <- Sys.getenv("DATABRICKS_TOKEN")
  
  if (token == "" || is.null(token)) {
    stop("DATABRICKS_TOKEN environment variable is not set. 
         Please set it using Sys.setenv(DATABRICKS_TOKEN = 'your-token') 
         or set it in your system environment variables.")
  }
  
  message("Successfully retrieved Databricks token from environment variable.")
  return(token)
}

databricks_token <- get_databricks_token()

# -----------------------------------------------------------------------------
# 4. Download and Configure JDBC Driver
# -----------------------------------------------------------------------------

# Check if JDBC driver exists
check_jdbc_driver <- function(driver_path) {
  jar_files <- list.files(driver_path, pattern = "\\.jar$", full.names = TRUE)
  
  if (length(jar_files) == 0) {
    stop(paste("No JAR files found in:", driver_path,
               "\nPlease ensure the Databricks JDBC driver is installed."))
  }
  
  message("Found JDBC driver files:")
  message(paste(" -", jar_files, collapse = "\n"))
  
  return(jar_files)
}

jdbc_jars <- check_jdbc_driver(databricks_config$jdbc_driver_path)

# -----------------------------------------------------------------------------
# 5. Create Database Connection
# -----------------------------------------------------------------------------

create_databricks_connection <- function(config, token) {
  
  # Build JDBC connection string
  connection_string <- paste0(
    "jdbc:databricks://", config$server_hostname, ":443/default;",
    #"jdbc:spark://", config$server_hostname, ":443/default;",
    "transportMode=http;",
    "ssl=1;",
    "httpPath=", config$http_path, ";",
    "AuthMech=3;",
    "EnableArrow=0;",
    "ThriftTransport=2;",
    "UseNativeQuery=1;",
    "UID=token;",
    "PWD=", token
  )
  print(connection_string) #testing within databricks connection function
  message("Creating connection details for Databricks...")
  
  # Create connection details using DatabaseConnector
  connection_details <- DatabaseConnector::createConnectionDetails(
    dbms = "spark",  # Databricks uses Spark SQL
    connectionString = connection_string,
    pathToDriver = config$jdbc_driver_path
  )
  
  return(connection_details)
}

# Alternative method using individual parameters
create_databricks_connection_v2 <- function(config, token) {
  
  message("Creating connection details for Databricks (alternative method)...")
  
  # For some versions, you might need to use this approach
  connection_details <- DatabaseConnector::createConnectionDetails(
    dbms = "spark",
    #server = paste0(config$server_hostname, "/default"),
    server = config$server_hostname,
    port = 443,
    user = "token",
    password = token,
    pathToDriver = config$jdbc_driver_path,
    extraSettings = paste0(
      "transportMode=http;ssl=1;httpPath=", config$http_path, ";AuthMech=3;ThriftTransport=2"
    )
  )
  
  return(connection_details)
}




# Create connection details
connection_details <- create_databricks_connection_v2(databricks_config, databricks_token)

# -----------------------------------------------------------------------------
# 6. Test Database Connection
# -----------------------------------------------------------------------------

test_connection <- function(conn_details, cdm_schema) {
  message("\nTesting database connection...")
  
  tryCatch({
    conn <- DatabaseConnector::connect(conn_details)
    
    # Test query - check for OMOP CDM tables
    test_query <- paste0(
      "SELECT COUNT(*) as row_count FROM ", cdm_schema, ".person LIMIT 1"
    )
    
    result <- DatabaseConnector::querySql(conn, test_query)
    message(paste("Connection successful! Person table contains approximately", 
                  result$ROW_COUNT[1], "records"))
    
    # List available tables
    tables_query <- paste0("SHOW TABLES IN ", cdm_schema)
    tables <- DatabaseConnector::querySql(conn, tables_query)
    message("\nAvailable tables in CDM schema:")
    print(head(tables, 20))
    
    DatabaseConnector::disconnect(conn)
    message("\nConnection test completed successfully!")
    return(TRUE)
    
  }, error = function(e) {
    message(paste("Connection test failed:", e$message))
    return(FALSE)
  })
}

print(connection_details) #for testing
# Run connection test
connection_success <- test_connection(connection_details, databricks_config$cdm_database_schema)

if (!connection_success) {
  stop("Failed to connect to database. Please check your configuration.")
}

# -----------------------------------------------------------------------------
# 7. Create Output Directory
# -----------------------------------------------------------------------------

if (!dir.exists(output_config$output_folder)) {
  dir.create(output_config$output_folder, recursive = TRUE)
  message(paste("Created output directory:", output_config$output_folder))
}

# -----------------------------------------------------------------------------
# 8. Run DataQualityDashboard
# -----------------------------------------------------------------------------

run_dqd <- function(conn_details, config, output_config) {
  
  message("\n========================================")
  message("Starting DataQualityDashboard Execution")
  message("========================================\n")
  
  # Get CDM version (typically 5.3 or 5.4)
  cdm_version <- "5.4"  # MODIFY if using different version
  
  message(paste("CDM Version:", cdm_version))
  message(paste("CDM Database Schema:", config$cdm_database_schema))
  message(paste("Vocabulary Schema:", config$vocab_database_schema))
  message(paste("Results Schema:", config$results_database_schema))
  message(paste("Output Folder:", output_config$output_folder))
  
  # Execute DQD
  results <- DataQualityDashboard::executeDqChecks(
    connectionDetails = conn_details,
    cdmDatabaseSchema = config$cdm_database_schema,
    resultsDatabaseSchema = config$results_database_schema,
    vocabDatabaseSchema = config$vocab_database_schema,
    cdmSourceName = config$cdm_source_name,
    cdmVersion = cdm_version,
    
    # Check levels to run
    checkLevels = c("TABLE", "FIELD", "CONCEPT"),
    
    # Check names (NULL runs all checks)
    checkNames = NULL,
    
    # Tables to exclude (if any)
    tablesToExclude = c(),
    
    # Output settings
    outputFolder = output_config$output_folder,
    outputFile = output_config$output_file,
    
    # Execution settings
    verboseMode = TRUE,
    writeToTable = TRUE,
    writeToCsv = TRUE,
    
    # Number of threads for parallel processing
    numThreads = 1,  # Increase if your Databricks cluster supports it
    
    # SQL only mode (set to FALSE to execute)
    sqlOnly = FALSE,
    sqlOnlyUnionCount = 1,
    sqlOnlyIncrementalInsert = FALSE
  )
  
  return(results)
}

# Execute DQD
dqd_results <- tryCatch({
  run_dqd(connection_details, databricks_config, output_config)
}, error = function(e) {
  message(paste("\nError running DQD:", e$message))
  message("\nTrying with reduced check set...")
  
  # Retry with a subset of checks if full run fails
  DataQualityDashboard::executeDqChecks(
    connectionDetails = connection_details,
    cdmDatabaseSchema = databricks_config$cdm_database_schema,
    resultsDatabaseSchema = databricks_config$results_database_schema,
    vocabDatabaseSchema = databricks_config$vocab_database_schema,
    cdmSourceName = databricks_config$cdm_source_name,
    cdmVersion = "5.4",
    checkLevels = c("TABLE", "FIELD"),  # Reduced check levels
    checkNames = NULL,
    tablesToExclude = c("note", "note_nlp", "specimen"),  # Exclude potentially problematic tables
    outputFolder = output_config$output_folder,
    outputFile = output_config$output_file,
    verboseMode = TRUE,
    writeToTable = FALSE,  # Don't write to table on retry
    writeToCsv = TRUE,
    numThreads = 1,
    sqlOnly = FALSE
  )
})

# -----------------------------------------------------------------------------
# 9. View Results Summary
# -----------------------------------------------------------------------------

summarize_results <- function(results_folder, results_file) {
  
  results_path <- file.path(results_folder, results_file)
  
  if (file.exists(results_path)) {
    message("\n========================================")
    message("DataQualityDashboard Results Summary")
    message("========================================\n")
    
    results_json <- jsonlite::fromJSON(results_path)
    
    # Overall summary
    if (!is.null(results_json$overview)) {
      message("Overview:")
      print(as.data.frame(results_json$overview))
    }
    
    # Check results summary
    if (!is.null(results_json$CheckResults)) {
      check_results <- as.data.frame(results_json$CheckResults)
      
      message("\nCheck Results Summary:")
      message(paste("Total Checks:", nrow(check_results)))
      
      if ("passed" %in% names(check_results)) {
        message(paste("Passed:", sum(check_results$passed == 1, na.rm = TRUE)))
        message(paste("Failed:", sum(check_results$passed == 0, na.rm = TRUE)))
      }
      
      if ("checkLevel" %in% names(check_results)) {
        message("\nChecks by Level:")
        print(table(check_results$checkLevel))
      }
      
      # Show failed checks
      if ("passed" %in% names(check_results)) {
        failed_checks <- check_results[check_results$passed == 0, ]
        if (nrow(failed_checks) > 0) {
          message("\nTop Failed Checks:")
          print(head(failed_checks[, c("checkName", "tableName", "fieldName", "numDenominatorRows", "numViolatedRows")], 20))
        }
      }
    }
    
    message(paste("\nFull results saved to:", results_path))
    
  } else {
    message(paste("Results file not found:", results_path))
  }
}

summarize_results(output_config$output_folder, output_config$output_file)

# -----------------------------------------------------------------------------
# 10. Launch Interactive Dashboard (Optional)
# -----------------------------------------------------------------------------

launch_dashboard <- function(results_folder, results_file) {
  results_path <- file.path(results_folder, results_file)
  
  if (file.exists(results_path)) {
    message("\nLaunching interactive DQD viewer...")
    DataQualityDashboard::viewDqDashboard(results_path)
  } else {
    message("Results file not found. Cannot launch dashboard.")
  }
}

# Uncomment to launch interactive dashboard
# launch_dashboard(output_config$output_folder, output_config$output_file)

# -----------------------------------------------------------------------------
# 11. Cleanup
# -----------------------------------------------------------------------------

message("\n========================================")
message("DataQualityDashboard Execution Complete")
message("========================================")
message(paste("\nResults Location:", file.path(output_config$output_folder, output_config$output_file)))
message("\nTo view the interactive dashboard, run:")
message(paste0('DataQualityDashboard::viewDqDashboard("', 
               file.path(output_config$output_folder, output_config$output_file), '")'))