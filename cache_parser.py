### Import dependencies
import json
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import re


'''
FUNCTIONS THAT READ FROM CACHE AND CREATE 'MASTER' DATAFRAME
'''


### FUNCTION: Opens the cache and read data to dictionary (by site)
def log_opener(file_path):
    print("Opening log: " + file_path)
    site_dict = {}     
    site_scanned = [] 

    with open(file_path) as f:
        line_num = 0
        for line in f:
            line_num += 1
            try:
                json_data = json.loads(line)  
                site = json_data["meta"]["request"]["domain"] 
                if site not in site_scanned:
                    site_dict[site] = [json_data]
                    site_scanned.append(site)
                else:
                    site_dict[site].append(json_data)
            except Exception as e:
                print(e)
                print("Skipped a line ({})! Check error log for bad data.".format(str(line_num)))

    print("Done opening log!")
    return site_dict


### FUNCTION: Creates DataFrame from opened cache
def df_creator(data_dict, df_merge_fields):

    # Master output table
    col_labels = ["site_url", "endpoint_category", "endpoint", "date", "value"]    
    master = pd.DataFrame(columns = col_labels)

    ### HELPER FUNCTION: Makes mini-dataframes for appending to 'master'
    def json_parser(list_of_jsons):

        # Create empty dataframe for appending site info to
        df = pd.DataFrame()

        # Go through each site's data; create site-specific DataFrame
        for metric in list_of_jsons:
            endpoint = [key for key in metric.keys() if key != "meta"][0]
            endpoint_category = metric["meta"]["request_parameters"]["endpoint_category"]
            data = [[site_name, endpoint_category, endpoint, d["date"], d[endpoint]] for d in metric[endpoint]]
            site_df = pd.DataFrame(data, columns = col_labels)
            df = df.append(site_df)    

        return df

    # Iterate through all of the sites
    for k, v in data_dict.items():
        jsons = v
        site_name = k
        master = master.append(json_parser(jsons)) 

    # Remove any duplicate rows
    print("Master's dimensions: " + str(master.shape))
    master.drop_duplicates(subset=["site_url",
    "endpoint_category", "endpoint", "date"], keep='last', inplace=True)
    print("Removing duplicates before transformations...")
    print("Any duplicated rows in dataframe? " + str(master.duplicated().any())
            + "!")
    print("Master's dimensions after duplicate removal: " + str(master.shape))

    ### HELPER FUNCTION: Does all transformations to 'master' 
    def transform(master):
        
        # Add other (non-API request) fields to data
        merged_master = master.merge(df_merge_fields, on="site_url")
       
        # Transform 'merged_master' (to import into GDS)
        master = merged_master.set_index(["group_site", "KA_initiative", "site_url", "site_name",
            "endpoint_category", "date", "endpoint"])

        # Move value fields to end, grouped by date
        master = master.unstack(level=-1).reset_index()
        master.columns = [' '.join(col).strip() if "value" not in col else
                list(filter(lambda x: x != "value", col))[0] for col in master.columns.values]
        
        # Add learning time (mins.) field
        master['LT_mins'] = master['visits'] * master['average_visit_duration'] / 60.0

        # GroupBy 'group_site'...
        master = master.groupby(['group_site', 'KA_initiative',
            'endpoint_category', 'date']).sum()
        master['average_visit_duration'] = master['LT_mins'] / master['visits']

        # Add normalized LT field
        master = normalize_LT(master)

        # Add TTM calculations
        master = TTMdf_joiner(master, pd.core.window.Rolling.sum,
                pd.core.window.Rolling.mean)

        # Add % Y/Y calculations
        master = yoyer(master) 

        # Add % M/M calculations
        master = momer(master) 

        # Drop useless columns
        cols_to_drop = [col for col in master.columns if col.find("pct_yoy_pct_mom") != -1]
        for col in cols_to_drop:
            del master[col]
        print('Dropped needless columns!')
       
        print('Done all dataframe manipulations!')
        print("Master's FINAL dimensions: " + str(master.shape))
        return master


    return transform(master)


'''
FUNCTIONS TRANSFORMING 'MASTER' DATAFRAME
'''

### FUNCTION: Add 'normalized_LT_by_KA' field
def normalize_LT(df):

    # Create indexed version of 'df'
    indexed_df = df

    # Fix 'lexsort' issue
    indexed_df = indexed_df.sort_index()

    # De-index 'df'
    df.reset_index(inplace=True)

    # Index of 'normalizer' site
    index_stem = ['KA (SimilarWeb)', 'Comp']

    # Write a helper function to then apply to each rows 
    def normalizer(df, x, index_stem, endpoint_category, date, column):
        normalizer_data = df.loc[tuple(index_stem + [endpoint_category] +
            [date]), [column]]
        return x / normalizer_data

    # Execute apply
    df['norm_LT'] = df.apply(lambda row: normalizer(indexed_df, row['LT_mins'], index_stem, row['endpoint_category'], row['date'], 'LT_mins'), axis=1)
    print("Done making 'normalized_LT' dataframe!")
    return df


### FUNCTION: Joins the TTM dataframes
def TTMdf_joiner(df, *args):

    indexed_df = df.set_index(['group_site',  'KA_initiative', 'endpoint_category', 'date'])
    
    ### HELPER FUNCTION: Add TTM fields
    def TTMer(df, func):

        indexed_df = df.set_index(['group_site',  'KA_initiative', 'endpoint_category', 'date'])

        # Save original indexed_df to join with calculated dataframe later
        og_indexed_df = indexed_df.copy()
        
        # Drop 'endpoint_category' to column; get keys to iterate through    
        indexed_df = indexed_df.reset_index(level=(-1,-2), inplace=False)
        indexed_df.sort_index(inplace=True)
        keys = list(set(indexed_df.index.values))

        # Turn off pandas' SettingWithCopyWarning
        pd.options.mode.chained_assignment = None

        # Do TTM calculations
        for key in keys:
            site_info = indexed_df.loc[key,:]
            site_info.set_index(['endpoint_category'], inplace=True)
            endpoint_categories = list(set(site_info.index.values))
            for endpoint_category in endpoint_categories:
                site_info.loc[endpoint_category] = func(site_info.loc[endpoint_category].rolling(window=12, min_periods=12))

        # Set indices for join
        indexed_df.reset_index(inplace=True)
        indexed_df.set_index(['group_site',  'KA_initiative', 'endpoint_category', 'date'], inplace=True)

        print('Successfully wrote TTM data!')
        return indexed_df

    # Generate 'TTMdf's; add to list
    TTMdf_dict = {}
    for func in args:
        TTMdf_dict[func.__name__] = TTMer(df, func)
    
    # Do joins into final dataframe
    for k, v in TTMdf_dict.items():
        indexed_df = indexed_df.join(v, how='left', rsuffix='_TTM_'+k)
    
    print('Successfully joined TTM dataframes!')
    return indexed_df.reset_index()


### FUNCTION: Calculates %Y/Y for every datafield
def yoyer(df):
    # Index the dataframe so only endpoints are column values
    indexed_df = df.set_index(['group_site',  'KA_initiative', 'endpoint_category', 'date'])
    indexed_df.sortlevel(inplace=True)

    #Helper function to perform % y/y calculation
    def yoy_calculator(df, row, col):
        key = list(row.name)
        curr_row_date = datetime.datetime.strptime(key.pop(), '%Y-%m-%d').date()
        base_date = curr_row_date + relativedelta(years=-1)
        base_key = tuple(key + [str(base_date)])
        if base_key in df.index:
            year_0 = df.loc[base_key, col]
            year_1 = df.loc[tuple(key+[str(curr_row_date)]), col]
            if year_0 != 0.0:
                try:
                    pct_chg = (year_1/year_0) - 1
                    return pct_chg
                except:
                    return
            else:
                return
        else:
            return

    # Execute apply of 'yoy_calculator' by row
    for col in indexed_df.columns:    
        indexed_df[col+'_pct_yoy'] = indexed_df.apply(lambda x: yoy_calculator(indexed_df, x, col), axis=1)

    # Return dataframe (unindexed)
    print('Successfully wrote %YOY columns!')
    return indexed_df.reset_index()


### FUNCTION: Calculates %Y/Y for every datafield
def momer(df):
    # Index the dataframe so only endpoints are column values
    indexed_df = df.set_index(['group_site', 'KA_initiative', 'endpoint_category', 'date'])
    indexed_df.sortlevel(inplace=True)

    #Helper function to perform % m/m calculation
    def mom_calculator(df, row, col):
        key = list(row.name)
        curr_row_date = datetime.datetime.strptime(key.pop(), '%Y-%m-%d').date()
        base_date = curr_row_date + relativedelta(months=-1)
        base_key = tuple(key + [str(base_date)])
        if base_key in df.index:
            month_0 = df.loc[base_key, col]
            month_1 = df.loc[tuple(key+[str(curr_row_date)]), col]
            if month_0 != 0.0:
                try:
                    pct_chg = (month_1/month_0) - 1
                    return pct_chg
                except:
                    return
            else:
                return
        else:
            return

    # Execute apply of 'mom_calculator' by row
    for col in indexed_df.columns:    
        indexed_df[col+'_pct_mom'] = indexed_df.apply(lambda x: mom_calculator(indexed_df, x, col), axis=1)

    # Return dataframe (unindexed)
    print('Successfully wrote %MOM columns!')
    return indexed_df.reset_index()

