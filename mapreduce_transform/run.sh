#!/bin/bash

#print service time
date

#virtualenv is now active
source /mnt/d/linux/final_project/venv/bin/activate

#running etl service
python3  /mnt/d/linux/final_project/appmapreduce.py

filetime=$(date +"%Y%m%d")
echo "[INFO] Mapreduce is Running ....."
#running mapreduce on local
python /mnt/d/linux/final_project/appmapreduce.py /mnt/d/linux/final_project/local/dim_ct_$filetime.csv > /mnt/d/linux/hadoop_batch_processing/mapreduce_transform/customercount_output_local_map.txt
# #running mapreduce hadoop
# python /mnt/d/linux/hadoop_batch_processing/mapreduce.py -r hadoop hdfs:///digitalskola/project/dim_orders_$filetime.csv > /mnt/d/linux/hadoop_batch_processing/output/ordercount_output_hadoop_map.txt

echo "[INFO] Mapreduce is Done ....."