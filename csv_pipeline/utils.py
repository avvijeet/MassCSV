Describe how you would ensure the pipeline performs well with increasing volumes of data.
How would you handle performance issues?
Reading CSV
Processing CSV
Inserting  into the DB
Bulk Upserts 
Error Handling and Monitoring:
Outline your approach for handling errors during extraction, transformation, and loading.
Describe how you would monitor the pipeline to ensure it runs smoothly.
Security and Compliance:
How would you ensure the data pipeline complies with data protection regulations?
Capability to mark certain columns as secure -> CHANGING COLUMNS CONFIG FOR TRANSFORMATIONS
PII data should be redacted
Loggers should redact data -> USING PATCH BASED CONTEXT MANAGEMENT
Data at rest should be encrypted -> AES or DFH
Data at transit should be encrypted HTTPS TLS1.2
Access restriction to CSVs - S3 access
Data should stay in 1 region only eg, US data should not be moved to india -> Make sure that its deployed correctly 
Describe any security measures you would implement.
Documentation and Maintenance:
What documentation would you create for this pipeline?
WHAT IS THIS?
WHY IS
How would you handle updates or changes to the pipeline?
READING AND WRITING CSV
COLUMNS FIXING
CONFIGURATION DYNAMIC
ROWS TO CONTROL
CLEANSING CSV
ERROR HANDLING -> WOULD NOT CHANGE CAPTURING ERRORS 
DO SOMETHING ABOUT THE ERROR
