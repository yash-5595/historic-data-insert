import os
import numpy as np
import glob
from multiprocessing import Process
import multiprocessing as mp
import pandas as pd
import  psycopg2
import subprocess
import sys
import psycopg2.extras as extras
import pandas.io.sql as psql


from datetime import datetime



class ISCProcessor:


    """
    A class with member functions to process and insert controller logs data to postgres database.

    ...

    Attributes
    ----------
    store_path : str
        Path where raw atspm files are stored
    convert_path : str
        Path where converted files are stored
    bit_mask_path : str
        Path where bit mask files are stored
    decoder_path: str
       Path where Purdue decoder (used to decode files) is stored.
    year: int
        Year
    month: int
        Month
    no_cores: int
        No of CPU cores to use.
        
    """

    def __init__(self):
        self.store_path = '/home/yash/interruptions/real_time_pipeline/Orlando'
        self.convert_path = '/home/yash/interruptions/real_time_pipeline/Orlando_converted'
        self.bit_mask_path = '/home/yash/interruptions/real_time_pipeline/Orlando_converted_bit_mask'
        self.raw_data_path = '/home/yash/interruptions/real_time_pipeline/Orlando_converted_raw_data'
        self.decoder_path = '/home/yash/interruptions/real_time_pipeline/PurdueDecoder.exe'
        
        self.year = 2022
        self.month = 11
        self.no_cores = 30
        
        self.all_days = glob.glob(f"{self.store_path}/{self.year}/{self.month}/*")
        
        
        
    def process_all_days(self):
        """
        Processes data all the days in parallel
        Parameters
        ----------
       
        Returns
        -------
        None
        """
        p = mp.Pool(self.no_cores)
        p.map(self.process_one_day, self.all_days)
        p.close()
    
    def process_one_day(self, one_day):
        """
        Processes data for all the intersections for a single day
        Parameters
        ----------
        one_day : str
            Path where the data for the day is stored

        Returns
        -------
        None
        """
        all_isc = glob.glob(f"{one_day}/*")

        
        for each_isc in all_isc:
            try:
                if('dat' not in each_isc  ):
                    print(f"start {each_isc} time {datetime.now()}")

                    isc_p.process_one_isc(each_isc)
                    print(f"end {each_isc} time {datetime.now()}")
            except Exception as e:
                print(f"Yash: fatal fAIL FOR ONE ISC ONE Day {each_isc} due to {e}")
        
        
        
        
    def process_one_isc(self, one_isc):
        """
        Processes raw files for a single intersection
        Parameters
        ----------
        one_isc : str
            Intersection ID

        Returns
        -------
        None
        """
#         print(f"doing for {one_isc}")
        all_files = glob.glob(f"{one_isc}/*")
        all_files.sort()
        ISC_NAME = ''
        bit_mask_res =[]
        raw_data = []
        for each_file in all_files:
            try:
                all_split = each_file.split('/')
                city, year, month, day, isc, filename  = all_split[5], all_split[6], all_split[7], all_split[8], all_split[9],all_split[10] 
                ISC_NAME = isc
                self.check_destination_directory(self.convert_path,year, month, day, isc)
                self.check_destination_directory(self.bit_mask_path,year, month, day, isc)
                self.check_destination_directory(self.raw_data_path,year, month, day, isc)
                out_file = f"{self.convert_path}/{year}/{month}/{day}/{isc}/{filename}.txt"
                temp = out_file.split('/')[-1].split('_')
                curr_ts = pd.to_datetime(f"{temp[2]}-{temp[3]}-{temp[4]} {temp[5][:2]}:{temp[5][2:4]}:00")

                process = subprocess.run(["wine", f"{self.decoder_path }", f"{each_file}", f"{out_file}"])
                bit_mask_res.append({'signalid':isc, 'timestamp':curr_ts.strftime("%Y-%m-%d %H:%M:%S"), 'flag':1})

                df_file = pd.read_csv(out_file,sep=',',skiprows = 6,header=None,names = ['timestamp','eventcode','eventparam'] )

                raw_data.append(df_file)
            except Exception as e:
                print(f"Yash:   **** decode failed for {each_file} error {e}")
        
        if(bit_mask_res):
            
            bit_mask = pd.DataFrame(bit_mask_res)
            raw_data_df = pd.concat(raw_data)
            raw_data_df.insert(0, 'signalid', ISC_NAME)

            bit_mask.to_csv(f"{self.bit_mask_path}/{year}/{month}/{day}/{isc}/bit_mask.csv", index = False)
            raw_data_df.to_csv(f"{self.raw_data_path}/{year}/{month}/{day}/{isc}/raw_data.csv", index = False)
        
#         inserting to db

#         self.execute_values(bit_mask, 'atspm_bit_mask')
#         self.execute_values(raw_data_df, 'atspmraw')
            
#         return bit_mask_res,raw_data
            
        
    def execute_values(self, df, table):
        """
        Using psycopg2.extras.execute_values() to insert the dataframe
        """
        # Create a list of tupples from the dataframe values
        conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="postgres")
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
            
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"&&&&&&&&&&&&&&& DATABASE ERROR ************************")
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            conn.close()
            return 1
            
#         print("execute_values() done")
        cursor.close()
        conn.close()
            
        
            
        
        
        
        
        
    def check_destination_directory( self,root, year, month, day, isc):

        """
       Check if a destination directory exists
        Parameters
        ----------
        root : str
           root path
        year: int
            Year
        month: int
            Month
        day: int
            Day
        isc: str
            Intersection ID

        Returns
        -------
        None
        """

        if not os.path.exists(f"{root}/{year}"):
            os.makedirs(f"{root}/{year}")
            
        if not os.path.exists(f"{root}/{year}/{month}"):
            os.makedirs(f"{root}/{year}/{month}")
            
        if not os.path.exists(f"{root}/{year}/{month}/{day}"):
            os.makedirs(f"{root}/{year}/{month}/{day}")
            
        if not os.path.exists(f"{root}/{year}/{month}/{day}/{isc}"):
            os.makedirs(f"{root}/{year}/{month}/{day}/{isc}")
                           
isc_p = ISCProcessor()

isc_p.process_all_days()