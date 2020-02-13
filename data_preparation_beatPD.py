# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 14:27:28 2020

@author: QUJ
"""
import pandas as pd
import numpy as np
import os

import re
import tarfile
import io
import csv
import warnings





# working directory
os.chdir(r'M:\beat_pd\Jingan')

warnings.filterwarnings('ignore')

pd.set_option("display.max_rows",300)
pd.set_option("display.max_columns",100)



# =============================================================================
# # loading data
# =============================================================================

# create a directory for ready data
ready_data_directory = 'ready_data'
if not os.path.exists(ready_data_directory):
    os.makedirs(ready_data_directory)



'''
encoding ancillary_data as 0 and training_data as 1 
'''

data_type_dict = {
        'ancillary_data': 0,
        'training_data': 1
        }

data_subsource_dict = {
        'smartphone_accelerometer': 0,
        'smartwatch_accelerometer': 1,
        'apple_watch': 2
        }

data_name_list = [] # record all data frame variable names of ready data

cis_sensor_data_list = [] 

real_sensor_data_list = []
real_gyro_data_list = []

# directory data is where the original data is saved
dirname = 'data'
filenames = os.listdir(dirname)
 




'''
clinical data and data labels
'''      
for filename in filenames:

    if filename.endswith('.csv'):
        continue
    file_path = os.path.join(dirname, filename)    
    split_pattern = '[-.]'     
    data_source, _, data_type, _, _ = re.split(split_pattern, filename)    
    #open tar file
    tar = tarfile.open(file_path)   
    if data_type == 'clinical_data':
        print(filename)
        #df_list = []
        for member in tar.getmembers():
            if member.isreg():
                #print("{} - {} bytes".format(member.name, member.size))
                csv_file = io.StringIO(tar.extractfile(member).read().decode('ascii'))
                lines = list(csv.reader(csv_file))                      
                
                header, *values = lines
                data_dict = {h: v for h, v in zip (header, zip(*values))}
                
                clinical_data = pd.DataFrame(data_dict)
                
                # other features
                file_name = member.name.split('/')[-1]
                #sub_pattern =  '(' + data_type + r'/)|(.csv)'
                #sub_pattern = r'[(ancillary_data/)(.csv)]'
                data_name = file_name.replace('.csv', '').replace('-', '_')
                
                exec(data_name + ' = clinical_data')
                
                # record the data name
                data_name_list.append(data_name)
                
                # save data
                clinical_data.to_csv(os.path.join('ready_data', file_name), index = False)               
         
    if data_type == 'data_labels':
        print(filename)        
        #df_list = []
        for member in tar.getmembers():
            if member.isreg():
                #print("{} - {} bytes".format(member.name, member.size))
                csv_file = io.StringIO(tar.extractfile(member).read().decode('ascii'))
                lines = list(csv.reader(csv_file))                      
                
                header, *values = lines
                data_dict = {h: v for h, v in zip (header, zip(*values))}
                
                data_labels = pd.DataFrame(data_dict)
                
                # other features
                file_name = member.name.split('/')[-1]
                #sub_pattern =  '(' + data_type + r'/)|(.csv)'
                #sub_pattern = r'[(ancillary_data/)(.csv)]'
                data_name = file_name.replace('.csv', '').replace('-', '_')
                
                exec(data_name + ' = data_labels')
                
                # record the data name
                #data_name_list.append(data_name)                    
                
                # save data
                #data_labels.to_csv(os.path.join('ready_data', file_name), index = False)                    

    # close tar file
    tar.close()    

# combine cis_id_label data
CIS_PD_Ancillary_Data_IDs_Labels['data_type'] = 0
CIS_PD_Training_Data_IDs_Labels['data_type'] = 1
cis_data_id_labels = pd.concat([CIS_PD_Ancillary_Data_IDs_Labels, CIS_PD_Training_Data_IDs_Labels],
                               ignore_index = True)
cis_data_id_labels['numeric_id'] = cis_data_id_labels.index
data_name_list.append('cis_data_id_labels')  
cis_data_id_labels.to_csv(r'ready_data/cis_data_id_labels.csv', index = False)



# combine real_id_label data
REAL_PD_Ancillary_Data_IDs_Labels['data_type'] = 0
REAL_PD_Training_Data_IDs_Labels['data_type'] = 1
real_data_id_labels = pd.concat([REAL_PD_Ancillary_Data_IDs_Labels, REAL_PD_Training_Data_IDs_Labels],
                                ignore_index = True)
real_data_id_labels['numeric_id'] = real_data_id_labels.index
data_name_list.append('real_data_id_labels')
real_data_id_labels['subsource'] = 0
#real_data_id_labels.to_csv(r'ready_data/real_data_id_labels.csv', index = False)



  
'''
cis sensor data 
'''
for filename in filenames:

    if filename.endswith('.csv'):
        continue
    file_path = os.path.join(dirname, filename)    
    split_pattern = '[-.]'     
    data_source, _, data_type, _, _ = re.split(split_pattern, filename)    
    #open tar file
    tar = tarfile.open(file_path) 
    if data_source == 'cis' and data_type in ['ancillary_data', 'training_data']:      
        print(filename)               
        for member in tar.getmembers():
            if member.isreg():
                print("{} - {} bytes".format(member.name, member.size))
                csv_file = io.StringIO(tar.extractfile(member).read().decode('ascii'))
                lines = list(csv.reader(csv_file))                      
                
                header, *values = lines
                data_dict = {h: v for h, v in zip (header, zip(*values))}
                
                sensor_data = pd.DataFrame(data_dict)
                
                # change to numeric columns
                for l in sensor_data.columns:
                    sensor_data[l] = pd.to_numeric(sensor_data[l])
                
                # other features
                
                file_name = member.name.split('/')[-1]
                #sub_pattern = '[(' + data_type + '/)(.csv)]'
                #sub_pattern = r'[(ancillary_data/)(.csv)]'
                measure_id = file_name.replace('.csv', '')
                
                #index = cis_data_id_labels.loc[cis_data_id_labels['measurement_id'] == measure_id]
                sensor_data['numeric_id'] = cis_data_id_labels.loc[cis_data_id_labels['measurement_id'] == measure_id, 'numeric_id'].iloc[0]
#                sensor_data['data_type'] = data_type_dict[data_type]
                # add data source feature if want to combine cis and real data together
                # sensor_data['data_source'] = data_source
                
                cis_sensor_data_list.append(sensor_data)

    # close tar file
    tar.close() 

# combine cis sensor data together
cis_sensor_data = pd.concat(cis_sensor_data_list, ignore_index = True) 
del cis_sensor_data_list
#cis_sensor_data.reset_index(inplace = True)
# num_cols = ['Timestamp', 'X', 'Y', 'Z']

#for col in num_cols:
#    print('convert column {} to numeric column'.format(col))
#    cis_sensor_data[col] = pd.to_numeric(cis_sensor_data[col])


# save data 
print('.............saving cis sensor data...............')
# ready_data is a directory for ready data
cis_sensor_data.to_feather(r'ready_data/cis_sensor_data')

                    
'''
real sensor data
'''
for filename in filenames:

    if filename.endswith('.csv'):
        continue
    file_path = os.path.join(dirname, filename)    
    split_pattern = '[-.]'     
    data_source, _, data_type, _, _ = re.split(split_pattern, filename)    
    #open tar file
    tar = tarfile.open(file_path) 
    if data_source == 'real' and data_type in ['ancillary_data', 'training_data']:
        print(filename)             
        for member in tar.getmembers():
            if member.isreg():
                print("{} - {} bytes".format(member.name, member.size))
                csv_file = io.StringIO(tar.extractfile(member).read().decode('ascii'))
                lines = list(csv.reader(csv_file))                      
                
                header, *values = lines
                data_dict = {h: v for h, v in zip (header, zip(*values))}
                
                sensor_data = pd.DataFrame(data_dict)
                
                # change to numeric columns
                for l in sensor_data.columns:
                    sensor_data[l] = pd.to_numeric(sensor_data[l])                
                
                # other features
                _, subsource, file_name = member.name.split('/')
                #sub_pattern = r'(/)|(.csv)'
                #sub_pattern = r'[(ancillary_data/)(.csv)]'
                measure_id = file_name.replace('.csv', '')
                
                index = real_data_id_labels.loc[real_data_id_labels['measurement_id'] == measure_id].index
                sensor_data['numeric_id'] = real_data_id_labels.loc[index, 'numeric_id'].iloc[0]
#                sensor_data['data_type'] = data_type_dict[data_type]
                
                if subsource == 'smartwatch_gyroscope':
                    real_gyro_data_list.append(sensor_data)
                else:
                    real_data_id_labels.loc[index, 'subsource'] = data_subsource_dict[subsource]                   
                    real_sensor_data_list.append(sensor_data)
                    
    # close tar file
    tar.close()  

# combine real sensor data together
real_sensor_data = pd.concat(real_sensor_data_list, ignore_index = True) 
real_sensor_data.columns = cis_sensor_data.columns
del real_sensor_data_list
# save data 
print('.............saving real sensor data...............')
# ready_data is a directory for ready data
real_sensor_data.to_feather(r'ready_data/real_sensor_data')


# combine real sensor data together
real_gyro_data = pd.concat(real_gyro_data_list, ignore_index = True) 
real_gyro_data.columns = cis_sensor_data.columns
del real_gyro_data_list
# save data 
print('.............saving real gyro data...............')
# ready_data is a directory for ready data
real_gyro_data.to_feather(r'ready_data/real_gyro_data')

                   
# save real label data
real_data_id_labels.to_csv(r'ready_data/real_data_id_labels.csv', index = False)




# =============================================================================
# data_name_list shows all the data frame variables we need 
# =============================================================================

data_name_list.extend(['cis_sensor_data', 'real_sensor_data', 'real_gyro_data'])

# and all data is saved in ready_data direcotry


















        

















































