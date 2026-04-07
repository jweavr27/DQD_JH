# CAIA OMOP DQD Validation - Complete Setup and Execution
# One script that does everything

cat("=== CAIA OMOP DQD Validation ===\n\n")

# Load required packages
required_packages <- c("DataQualityDashboard", "DatabaseConnector", "yaml", "jsonlite")
missing_packages <- setdiff(required_packages, rownames(installed.packages()))

if (length(missing_packages) > 0) {
  cat("Installing missing packages:", paste(missing_packages, collapse = ", "), "\n")
  install.packages("remotes")
  for (pkg in missing_packages) {
    if (pkg == "DataQualityDashboard") {
      remotes::install_github("OHDSI/DataQualityDashboard")
    } else {
      install.packages(pkg)
    }
  }
}

suppressMessages({
  library(DataQualityDashboard)
  library(DatabaseConnector)
  library(yaml)
  library(jsonlite)
})

# Load and validate configuration
if (!file.exists("config_template.yaml")) {
  stop("Config file 'config_template.yaml' not found. Please create it with your database details.")
}

config <- yaml::read_yaml("config_template.yaml")

# Validate required fields
required_fields <- c("dbms", "server", "database", "user", "password", "schema", "site_name")
missing_fields <- setdiff(required_fields, names(config))
if (length(missing_fields) > 0) {
  stop(paste("Missing config fields:", paste(missing_fields, collapse = ", ")))
}

cat("Configuration loaded for site:", config$site_name, "\n")
cat("Platform:", config$dbms, "\n")
cat("Database:", config$database, "\n\n")

# Create connection details
connection_details <- createConnectionDetails(
  dbms = config$dbms,
  server = config$server,
  database = config$database,
  user = config$user,
  password = config$password,
  port = config$port
)

# Test connection
cat("Testing database connection...\n")
tryCatch({
  conn <- connect(connection_details)
  test_query <- paste("SELECT COUNT(*) as cnt FROM", config$schema, ".person")
  result <- querySql(conn, test_query)
  disconnect(conn)
  cat("✅ Connection successful -", format(result$CNT, big.mark = ","), "persons found\n\n")
}, error = function(e) {
  cat("❌ Connection failed:", e$message, "\n")
  cat("Check your config_template.yaml settings and try again.\n")
  stop("Cannot proceed without database connection")
})

# Setup results directory
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
results_dir <- file.path("results", paste0("caia_", config$site_name, "_", timestamp))
dir.create(results_dir, recursive = TRUE)

# Configure DQD for CAIA requirements
cat("Configuring DQD for CAIA (7 tables only)...\n")

# Tables to exclude (keep only CAIA required tables)
all_tables <- c("person", "observation_period", "visit_occurrence", "visit_detail",
                "condition_occurrence", "drug_exposure", "procedure_occurrence", 
                "device_exposure", "measurement", "observation", "death", "note",
                "note_nlp", "specimen", "fact_relationship", "location", "care_site",
                "provider", "payer_plan_period", "cost", "drug_era", "dose_era",
                "condition_era", "episode", "episode_event", "metadata", "cdm_source")

caia_tables <- c("person", "death", "visit_occurrence", "condition_occurrence", 
                 "drug_exposure", "procedure_occurrence", "measurement")
tables_to_exclude <- setdiff(all_tables, caia_tables)

cat("Including:", paste(caia_tables, collapse = ", "), "\n")
cat("Excluding", length(tables_to_exclude), "other tables\n\n")

# Execute DQD
cat("Starting DQD execution (this takes 30-90 minutes)...\n")
start_time <- Sys.time()

tryCatch({
  executeDqChecks(
    connectionDetails = connection_details,
    cdmDatabaseSchema = config$schema,
    resultsDatabaseSchema = config$schema,
    cdmSourceName = paste0("CAIA_", config$site_name),
    cdmVersion = "5.4",
    outputFolder = results_dir,
    outputFile = "DataQualityResults.json",
    verboseMode = FALSE,
    writeToTable = FALSE,
    checkLevels = c("TABLE", "FIELD", "CONCEPT"),
    tablesToExclude = tables_to_exclude
  )
  
  # Generate HTML dashboard
  viewDqDashboard(
    jsonPath = file.path(results_dir, "DataQualityResults.json"),
    outputPath = results_dir
  )
  
  duration <- round(as.numeric(difftime(Sys.time(), start_time, units = "mins")), 1)
  cat("✅ DQD completed in", duration, "minutes\n\n")
  
}, error = function(e) {
  cat("❌ DQD execution failed:", e$message, "\n")
  stop("DQD execution failed - check error message above")
})

# Generate CAIA summary
cat("Creating CAIA summary...\n")
dqd_results <- fromJSON(file.path(results_dir, "DataQualityResults.json"))

# Create summary
summary_lines <- c(
  "CAIA OMOP DQD Validation Summary",
  "================================",
  paste("Site:", config$site_name),
  paste("Date:", Sys.Date()),
  paste("Platform:", config$dbms),
  paste("Execution Time:", duration, "minutes"),
  "",
  "Overall Results:",
  paste("Total Checks:", nrow(dqd_results$CheckResults)),
  paste("Passed:", sum(dqd_results$CheckResults$passed == 1, na.rm = TRUE)),
  paste("Failed:", sum(dqd_results$CheckResults$passed == 0, na.rm = TRUE)),
  "",
  "Submission Files:",
  "- DataQualityDashboard.html",
  "- DataQualityResults.json", 
  "- caia_summary.txt"
)

writeLines(summary_lines, file.path(results_dir, "caia_summary.txt"))

# Final output
cat("🎉 CAIA validation complete!\n\n")
cat("📁 Results location:", results_dir, "\n")
cat("📄 Files to submit:\n")
cat("   - DataQualityDashboard.html (main report)\n")
cat("   - DataQualityResults.json (data)\n")
cat("   - caia_summary.txt (summary)\n\n")
cat("📦 Create zip file: DQD_Results_", config$site_name, "_", format(Sys.Date(), "%Y%m%d"), ".zip\n")
cat("📧 Submit to CAIA coordinating center\n")