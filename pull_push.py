'''
SETTING UP THE ENVIRONMENT
'''

### Import dependencies
import gspread
from urllib.request import urlopen
import json
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import os
from oauth2client.service_account import ServiceAccountCredentials
import re
import cache_parser as cp
import shutil
from pandas.io import gbq


### OAuth2 credentialing and authentication; give gspread, gbq permissions 
scopes = ['https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
        "https://www.googleapis.com/auth/bigquery.insertdata",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform"]
credentials = ServiceAccountCredentials.from_json_keyfile_name('./ka_cred.json', scopes=scopes)
client = gspread.authorize(credentials)

### Load input parameters for API calls 
sheet = client.open("data_parameters").sheet1
vals = sheet.get_all_values()
headers = vals.pop(0)

# Find latest cache, outfile
file_re = re.compile(r'logs\/.*')
cells = sheet.findall(file_re)
cell_list = [(cell.row, cell.col) for cell in cells]
latest_log_coords = max(cell_list, key=(lambda item: item[0]))

file_re = re.compile(r'outfiles\/.*')
cells = sheet.findall(file_re)
cell_list = [(cell.row, cell.col) for cell in cells]
latest_csv_coords = max(cell_list, key=(lambda item: item[0]))

# Set up dataframe
df = pd.DataFrame(vals, columns=headers)
df.replace('',np.nan, inplace=True)

# Get list of fields to merge before returning 'master'
merge_fields = ["site_url", "site_name", "group_site", "KA_initiative"]

# Get DataFrame with added fields to merge with 'master' before writing to CSV
df_merge_fields = df.loc[:, merge_fields]

# Get sites, endpoint categories, versions, endpoints, granularity
parameters = {header:[params for params in df[header].dropna().values] for header in headers}
df_map = df[["endpoint_category", "version"]].set_index(["endpoint_category"])
endpoint_cat_version_map = df_map.to_dict()["version"]

# Get last log, outfile filenames
filename_last_log = parameters[headers[headers.index("log")]][-1] 
filename_last_csv = parameters[headers[headers.index("outfile")]][-1] 

# Set BigQuery table name
BQ_table_name = "ravi.cad_data"


'''
FUNCTIONS TO DO THE 'PULLING' AND 'PUSHING'
'''


def write_to_log(sites, endpoint_categories, endpoints):

    # End date is the most recent month; adjust for time in month.
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
   
    # Give SimilarWeb time to update database (15 days)
    if today.day > 15:
        end_date = lastMonth.strftime("%Y-%m")
    else:
        lastMonth = lastMonth + relativedelta(months=-1)
        end_date = lastMonth.strftime("%Y-%m")

    # MUST HAVE AT LEAST 'data_start.txt'!
    start_date_obj = lastMonth + relativedelta(months=-24)
    if not os.path.isfile(filename_last_log) or os.stat(filename_last_log).st_size == 0:
        log = filename_last_log
        flag_new = False
    else:
        now = datetime.datetime.now()
        log = "logs/" + now.strftime("%Y-%m-%d_%H:%M") + ".txt"
        flag_new = True

    start_date = start_date_obj.strftime("%Y-%m")

    print("Pulling data from: " + start_date)
    print("...to: " + end_date)

    # Get SimilarWeb API Key
    with open("sw_cred.txt", 'r') as f:
        API_key = f.readline().replace('\n','')

    ### HELPER FUNCTION: Extract data, unless error (writes to error log)
    def extractor(site, endpoint_category, version, endpoint, start_date, end_date, granularity="monthly"):

        # API static input variables
        domain = "https://api.similarweb.com/"
        out_form = "&format=json"

        # Call API; return data as JSON
        API_link = domain + version + "/" + "website/" + site + "/" + endpoint_category+ "/" + endpoint + API_key + "&start_date=" + start_date + "&end_date=" + end_date + "&main_domain_only=false" + "&granularity=" + granularity + out_form  
        
        # Attempt the API request
        try:
            response = urlopen(API_link)
            d = json.load(response)

        except Exception as e: 
            d = {"meta": {"status": "ERROR"}}
            print(e)

        # Add parameter information to the JSON request output
        d["meta"]["request_parameters"] = {"site": site, "endpoint_category" :
                endpoint_category, "endpoint": endpoint}

        # Error checking
        if d["meta"]["status"] != "Success":
            # Writing error into error log 
            print("ERROR HAPPENED!")
            d["time"] = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")
            with open('logs/extraction_error_log.txt','a+') as outfile:
                outfile.write(json.dumps(d["meta"]))
                outfile.write("\n")
                print("Wrote error to error log!")
        else:   
            return d
    
    # Generate log data
    all_json_data = []
    for site in sites:
        for endpoint_category in endpoint_categories:
            for endpoint in endpoints:
                extracted_data = extractor(site, endpoint_category, endpoint_cat_version_map[endpoint_category], endpoint, start_date, end_date) 
                all_json_data.append(extracted_data)

    # Write all log data to log file
    with open(log, 'a+') as f:
        for line in all_json_data:
            f.write(json.dumps(line))
            f.write("\n")

    # Update 'data_parameters'; combine log histories if incremental update
    if flag_new:
        
        # Copy 'filename_last_log' file; append lastest log
        combined_log = log[:-4] + "_concat.txt" 
        
        # Concatenate 'combined_log' and 'log'
        filenames = [filename_last_log, log]
        with open(combined_log, 'w') as outfile:
            for fname in filenames:
                with open(fname) as infile:
                    for line in infile:
                        outfile.write(line)

        # Update 'data_parameters' with 'log' and 'combined_log'
        sheet.update_cell(latest_log_coords[0]+1, 1, log)
        sheet.update_cell(latest_log_coords[0]+2, 1, combined_log)
        
        print("Log filenames written to 'data_parameters'!")
        print("Done writing both logs: " + log + ", " + combined_log)
        return combined_log 
    else:
        print("Done writing to new, 24-month log: " + log)
        return log 


def write_to_outfile(df):
    
    # Checks last ouput CSV; MUST HAVE AT LEAST 'out_start.csv'!
    if not os.path.isfile(filename_last_csv) or os.stat(filename_last_csv).st_size == 0:
        csv = filename_last_csv
        flag_new = False
    else:
        now = datetime.datetime.now()
        csv = "outfiles/" + now.strftime("%Y-%m-%d_%H:%M") + ".csv"
        flag_new = True

    with open(csv, 'a+') as outfile:
        # Writes the DataFrame to a CSV
        df.to_csv(outfile, header=True, index=False)

    # Write NEW filename to 'data_parameters' under most recent outfile
    if flag_new:
        sheet.update_cell(latest_csv_coords[0]+1, 2, csv)
        print("Outfile filename written to 'data_parameters'!")
   
    print("Done writing to outfile: " + csv)
    return df


'''
EXECUTION OF SCRIPT
'''


if __name__ == "__main__":
   
    # Flow control
    requests_on = False
    log_to_out_on = False
    BQ_write_on = False

    # SWITCH: API Requests
    if requests_on:
        log_file = write_to_log(parameters["site_url"], parameters["endpoint_category"], parameters["endpoint"]) 
    else:
        log_file = filename_last_log

    # Generate 'master' for writing
    df = cp.df_creator(cp.log_opener(log_file), df_merge_fields)

    # SWITCH: Writing to outfile
    if log_to_out_on:
        # Write to outfile, pass on 'master' to be written into BQ
        BQ_df = write_to_outfile(df)
        print("Opening newly created outfile!")
    else:
        BQ_df = pd.read_csv(filename_last_csv) 
        print("Opening existing outfile: " + filename_last_csv)

    # SWITCH: Write to BigQuery table
    if BQ_write_on:
        print("Preparing to stream into BigQuery!")
        gbq.to_gbq(BQ_df, BQ_table_name, "khanacademy.org:deductive-jet-827",
                chunksize=5000, verbose=True, reauth=False,
                if_exists='replace', private_key="./ka_cred.json")
        print("Done writing to BQ!")

