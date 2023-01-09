#store_path
#iterate through folders
#docker execute command 

#cd back back
store_path=/home/yash/interruptions/real_time_pipeline/Orlando_converted_raw_data/2022/12
mainDir=Orlando_converted_raw_data/2022/12
subs=`ls $mainDir`

for each_day in $subs; do
    #echo $each_day
    if (($each_day > 15));then
	    echo $each_day
	
    # cd $each_day
    	sub_isc=`ls $mainDir/$each_day`
    	for each_isc in $sub_isc; do
        # cd $each_isc
	echo $each_isc
	file=$mainDir/$each_day/$each_isc/raw_data.csv
	if test -f "$file";then
        	cmd=`ls -l -h $mainDir/$each_day/$each_isc/raw_data.csv`
		#psql atspm yash -c "\copy rawdata2022 from '$i' CSV HEADER;"
		docker cp $mainDir/$each_day/$each_isc/raw_data.csv postgres:/
		docker exec -it postgres bash -c 'psql $POSTGRES_DB $POSTGRES_USER -c "\copy atspmrawv2 from /raw_data.csv CSV HEADER;"'
		docker exec -it postgres bash -c 'rm /raw_data.csv'
		mv $mainDir/$each_day/$each_isc/raw_data.csv $mainDir/$each_day/$each_isc/raw_data_inserted.csv
        #echo $cmd
	fi
    
    done
    fi
done
