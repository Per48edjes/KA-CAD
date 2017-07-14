import json
import pandas as pd


### FUNCTION: Creates DataFrame from opened cache
def df_creator(data_dict, df_merge_fields):

    # Master output table
    col_labels = ["site_url", "endpoint_category", "endpoint", "date", "value"]    
    master = pd.DataFrame(columns = col_labels)

    # HELPER FUNCTION: Makes mini-dataframes for appending to 'master'
    def json_parser(list_of_jsons):

        # Create empty dataframe for appending site info to
        df = pd.DataFrame()

        # Go through each site's data; create site-specific DataFrame
        for metric in list_of_jsons:
            endpoint = [key for key in metric.keys() if key != "meta"][0]
            endpoint_category = metric["meta"]["request_parameters"]["endpoint_category"]
            '''
            Table structure:
            SITE_NAME ... ENDPOINT CATEGORY ... ENDPOINT ... DATE ... VALUE
            '''
            data = [[site_name, endpoint_category, endpoint, d["date"], d[endpoint]] for d in metric[endpoint]]
            site_df = pd.DataFrame(data, columns = col_labels)
            df = df.append(site_df)    

        return df

    # Iterate through all of the sites
    for k, v in data_dict.items():
        jsons = v
        site_name = k
        master = master.append(json_parser(jsons)) 

    # Add other (non-API request) fields to data
    merged_master = master.merge(df_merge_fields, on="site_url")

    # Transform 'merged_master' (to import into GDS)
    master = merged_master.set_index(["group_site", "KA_initiative", "site_url", "site_name",
        "endpoint_category", "date", "endpoint"])

    # Move value fields to end, grouped by date
    master = master.unstack(level=-1).reset_index()
    master.columns = [' '.join(col).strip() if "value" not in col else
            list(filter(lambda x: x != "value", col))[0] for col in master.columns.values]

    print("Done making master DataFrame!")
    return master


### FUNCTION: Opens the cache and read data to dictionary (by site)
def log_opener(file_path):
    site_dict = {}     
    last_site_scanned = None 

    with open(file_path) as f:
        for line in f:
            try:
                json_data = json.loads(line)  
                site = json_data["meta"]["request"]["domain"] 
                if site != last_site_scanned:
                    site_dict[site] = [json_data]
                else:
                    site_dict[site].append(json_data)
                last_site_scanned = site
            except:
                print("Skipped a line! Check error log for bad data.")

    print("Done opening log!")
    return site_dict

