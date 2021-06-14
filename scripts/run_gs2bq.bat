@echo off

call vars.bat

call java -jar ../target/dlp-example.jar ^
    --project=sabre-cdw-dev-sandbox --region=us-central1 --jobName=%JOB_NAME% ^
    --inputFile=gs://cdm-dlp-example/input/in.long.txt ^
    --dataset=dlp_example ^
    --gcpTempLocation=gs://cdm-dlp-example/temp ^
    --stagingLocation=gs://cdm-dlp-example/staging
