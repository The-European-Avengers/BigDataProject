#!/bin/bash

# Define the base directories
BASE_DIRS="/raw /historical /live /analytics"

# Define the years for the /historical directory
YEARS=$(seq 2020 2024)

# Define the topics for /historical and /live
TOPICS="weather-wind weather-temp weather-sun"

# Define the initial structure under /raw (based on the final tree overview)
RAW_SUBDIRS="/raw/initial /raw/forecast/weather-wind /raw/forecast/weather-temp /raw/forecast/weather-sun /raw/price /raw/historical/weather-wind /raw/historical/weather-temp /raw/historical/weather-sun"

echo "Starting creation of HDFS directory structure..."

# Step 1: Create the base directories: /raw, /historical, /live, /analytics
echo "Creating base directories: $BASE_DIRS"
# Use -p to prevent failure if directories already exist.
hdfs dfs -mkdir -p $BASE_DIRS

if [ $? -eq 0 ]; then
    echo "Base directories created successfully."
else
    echo "Error creating base directories. Aborting."
    exit 1
fi

# Step 2: Create nested subdirectories under /historical
echo "Creating /historical/YYYY/topic structure"
for year in $YEARS; do
    for topic in $TOPICS; do
        PATH_TO_CREATE="/historical/$year/$topic"
        echo "  -> Creating $PATH_TO_CREATE"
        hdfs dfs -mkdir -p "$PATH_TO_CREATE"
    done
done
echo "/historical structure completed."

# Step 3: Create subdirectories under /live
echo "Creating /live/topic structure"
for topic in $TOPICS; do
    PATH_TO_CREATE="/live/$topic"
    echo "  -> Creating $PATH_TO_CREATE"
    hdfs dfs -mkdir -p "$PATH_TO_CREATE"
done
echo "/live structure completed."

# Step 4: Create the extended structure under /raw
echo "Creating subdirectories in /raw"
for raw_subdir in $RAW_SUBDIRS; do
    echo "  -> Creating $raw_subdir"
    hdfs dfs -mkdir -p "$raw_subdir"
done

hdfs dfs -mkdir -p /raw/initial/weather-wind
hdfs dfs -mkdir -p /raw/initial/weather-temp
hdfs dfs -mkdir -p /raw/initial/weather-sun
echo "/raw structure completed."


echo "Verifying root directories in HDFS (/):"
hdfs dfs -ls /

echo "Verifying /historical structure:"
hdfs dfs -ls -R /historical

echo "Verifying /live structure:"
hdfs dfs -ls -R /live

echo "Verifying /raw structure:"
hdfs dfs -ls -R /raw

echo "HDFS structure creation script finished."