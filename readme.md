# 1) offline_isc_processor.py
    ## Python script for processing encoded(.dat) controller log  files (atspm)
    Converts dat files and combines all of them to a single csv file (one for each intersection for each day). Also creates bit mask for identifying periods of missing data

# 2) insert_data_to_db.sh 
     ## shell script for inserting the above generated csv files to postgres tables in the docker container  (controller log files/atspm )

# 3) insert_mask_to_db.sh 
     ## shell script for inserting the bit mask information to postgres tables in the docker container 