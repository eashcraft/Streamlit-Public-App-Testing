import http.client
import requests
import json
from json import JSONDecodeError
import pandas as pd
import numpy as np
import streamlit as st
import datetime
from io import BytesIO
import re
import aiohttp
import asyncio
from requests.exceptions import HTTPError, RequestException
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


st.title("Product Data Streamlit Apps")

# Dropdown menu for navigation
selected_page = st.selectbox("Select a Page", ["API Queries", "Model and Part Matching"])


# If user chooses this on selector, script will open options for API queries
if selected_page == "API Queries":
    st.header("Query Parts by Manufacturer via Pimberly API")
    st.markdown("###### Specify a manufacturer prefix then press enter. Script will ping the Pimberly API to pull all part data, and create a smart manual link dictionary.")
    #st.write("Welcome to the Home page!")


    # Streamlit input fields
    manufacturer_code = st.text_input("Enter Manufacturer Code:", "APW")
    max_concurrent_requests = st.slider("Max Concurrent Requests:", min_value=1, max_value=20, value=15)

    # File download button (will appear after processing)
    def convert_df_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        output.seek(0)  # Move the pointer to the beginning of the stream
        return output


    # Main execution block
    if st.button("Fetch Data"):
        try:
            # Record start time
            start_time = datetime.datetime.now()

            # Let user know script is kicking off (in case they ignore the "Running" icon in the top right)
            st.write(f"Gathering Part Links...")

            prim_id_list = []

            base_url = "https://app.us.pimberly.com/api/v2.2/products"

            # Define headers
            headers = {
                'Authorization': 'VTkxRkIVRkwIBdjaMFfXXKnD01LHE4APUFBN7DKltTUowG4HMUiInvUwNOzu1roh'
            }

            ### Passing query parameter (MFG code) as dictionary
            params = {"filters": json.dumps({"manufacturerCode": manufacturer_code})}

            @st.cache_data
            # Function to fetch data from a given URL with query parameters
            def fetch_data(url, params):
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()  # Raise an error if the request fails
                return response.json()

            # Fetch the initial data
            json_result = fetch_data(base_url, params)
            #st.write("Initial response:", json_result)  # Debug initial response

            product_id_result = json_result['data']
            prim_id_list.extend([p['primaryId'] for p in product_id_result])

            # Check if there's a next link to continue scrolling through pages
            next_link = json_result.get('next')

            # Cycle through "next" links to get the full product list
            while next_link:
                try:
                    # Append filters if not present in next_link
                    if "filters" not in next_link:
                        next_link = f"{next_link}&filters={json.dumps({'manufacturerCode': manufacturer_code})}"
                    
                    #st.write("Fetching next link:", next_link)  # Debug pagination (just for testing, muted because it prints out too many links)

                    json_result = fetch_data(next_link, params=None)
                except HTTPError:
                    #st.error("HTTP Error encountered while fetching the next link.")
                    break

                product_id_result = json_result['data']
                prim_id_list.extend([p['primaryId'] for p in product_id_result])

                next_link = json_result.get('next')

                ### Temporarily adding this in so I can track progress (just for testing, muted because it prints out too many links)
                #st.write(f"Parts found so far: {len(prim_id_list)}")

            prim_id_list = list(set(prim_id_list))  # Ensure unique IDs
            st.write(f"Total unique parts found: {len(prim_id_list)}")

            ### Fetch attributes for all IDs

            # Replace slashes in IDs
            prim_id_list = [p.replace("/", "%2F") for p in prim_id_list]

            # Convert product IDs into URLs
            p_id_list = [f"https://app.us.pimberly.com/api/v2.2/products/{p}/attributes" for p in prim_id_list]

            # Define the semaphore for controlling concurrent requests
            semaphore = asyncio.Semaphore(max_concurrent_requests)

            # Progress bar
            progress = st.progress(0)

            # Define the asynchronous function to fetch data from an API
            async def fetch(session, url, headers, progress_tracker):
                async with semaphore:
                    try:
                        async with session.get(url, headers=headers) as response:
                            response.raise_for_status()
                            progress_tracker["completed"] += 1
                            progress.progress(progress_tracker["completed"] / progress_tracker["total"])
                            return await response.json()
                    except Exception as e:
                        st.error(f"Error fetching URL {url}: {e}")
                        return None

            # Define the main function to run multiple asynchronous requests
            async def fetch_all(urls, headers):
                results = []
                progress_tracker = {"completed": 0, "total": len(urls)}
                async with aiohttp.ClientSession() as session:
                    tasks = [fetch(session, url, headers, progress_tracker) for url in urls]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                return [r for r in results if r is not None]

            # Use asyncio with new_event_loop in Streamlit
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = loop.run_until_complete(fetch_all(p_id_list, headers))
            finally:
                loop.close()

            # Convert results to Pandas DataFrame
            result_df = pd.DataFrame.from_dict(results)
            #st.write("Fetched Data:")
            #st.dataframe(result_df)

            # Timing how long this actually took
            end_time = datetime.datetime.now()
            execution_time = end_time - start_time
            st.success(f"Execution time: {execution_time}")

             # Convert DataFrame to Excel, make download button in Streamlit so you can get file
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")

            # Filtering out models from the output df (Tilde negates boolean series)
            result_df = result_df[~result_df["primaryId"].str.contains("PT_CAT", na=False)]

            # Filtering out columns we don't need
            try:
                result_df = result_df[['id', 'primaryId', 'classifications', 'approvalStatus', 'chainOnly', 'costPrice', 'countryOfOrigin', 'dimensionUOM', 'hybrisProductType',
                                       'imageType', 'isDropShip', 'listPrice', 'longDescription', 'manufacturerCatCode', 'manufacturerName', 'manufacturerPartNumber',
                                       'marketplacePart', 'obsoleteStatusChangeDate', 'onlineFlag', 'origin', 'salesNote', 'title','withholdFromSAP', 'manufacturerCode', 
                                       'parentManufacturer', 'obsolete', 'allowedForSAP', 'webPrice', 'optimizedDesc', 'purchasingNote','replacementProduct']]
            except:
                # Adding exception here because it looks like the API can drop columns that have entirely null values
                result_df = result_df[['id', 'primaryId', 'classifications', 'approvalStatus', 'chainOnly', 'costPrice', 'countryOfOrigin', 'dimensionUOM', 'hybrisProductType',
                                       'imageType', 'isDropShip', 'listPrice', 'longDescription', 'manufacturerCatCode', 'manufacturerName', 'manufacturerPartNumber',
                                       'marketplacePart', 'obsoleteStatusChangeDate', 'onlineFlag', 'origin', 'salesNote', 'title','withholdFromSAP', 'manufacturerCode', 
                                       'obsolete', 'allowedForSAP', 'optimizedDesc', 'purchasingNote']]

                result_df['replacementProduct'] = np.nan
                result_df['webPrice'] = np.nan
                result_df['parentManufacturer'] = np.nan


            excel_data = convert_df_to_excel(result_df)

            # Creating smart manual link dictionary
            def create_mfg_uri(mfg_name):
                url_name = mfg_name.lower()
                url_name = url_name.replace("&", "and")
                url_name = re.sub(r'[^\w\s-]', '', url_name)
                url_name = re.sub(r'\s+', '-', url_name)
                url_name = url_name.strip('-')
                return url_name


            # Filtering out obsolete parts that have no replacement (so there's no 404s/broken links in manual)
            active_df = result_df[result_df['obsolete'] == False]
            obs_replby_df = result_df[(result_df['replacementProduct'].notnull()) & (result_df['obsolete'] == True)]

            link_dict_df = pd.concat([active_df,obs_replby_df])

            link_dict_df = link_dict_df[['primaryId','manufacturerName', 'manufacturerPartNumber']]
            link_dict_df['manufacturerName'] = link_dict_df['manufacturerName'].apply(create_mfg_uri)

            csv_output_name = link_dict_df['manufacturerName'].iloc[0] + ".csv"

            # Ensures leading zeroes aren't dropped by converting any potential int to str
            link_dict_df['manufacturerPartNumber'] = link_dict_df['manufacturerPartNumber'].apply(lambda x: str(x))

            # Creating smart manual link in correct formatting necessary to use Adobe plugin
            link_dict_df['URL'] = link_dict_df.apply(lambda row:f" uri:http://www.partstown.com/{row.manufacturerName}/{row.primaryId}?pt-manual=AutoBatch", axis = 1)

            link_dict_df = link_dict_df[['manufacturerPartNumber','URL']]


            # Convert link dict df to a csv
            csv_data = link_dict_df.to_csv(index=False).encode("utf-8")

            # Creating download button for main part data
            st.download_button(
                label="Download Part Data",
                data=excel_data,
                file_name=f"Pimberly_API_Query-{manufacturer_code}_{current_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            # Creating download button for smart manual link dictionaries
            st.download_button(
                label="Download Smart Manual Link Dictionary",
                data=csv_data,
                file_name=csv_output_name,
                mime="text/csv",
            )

        except Exception as e:
            st.error(f"An error occurred: {e}")



# If user chooses this on selector, script will open options for model/part matching
if selected_page == "Model and Part Matching":
    st.header("Model and Part Matching")
    st.markdown("###### This app will accept any .csv or .xlsx file. Please ensure the column with manufacturer listed is called 'Customer_Mfg_Name', and the column with models is called 'Customer_Model_Name'.")

    def all_purpose_cleaner(x):
        x = str(x).lower().strip()

        for spec_char in list("[@_!#$%^&*()<>?/\|}{~:].,-' "):
            x = x.replace(spec_char,"")
        
        ### I hate Excel corruption so very,very much
        ### Will include this later as a round 2 match process in case the part matches exactly with leading zero version
        #lowercase_part = lowercase_part.lstrip("0")

        return x



    def mfg_fluff_scrubber(some_mfg):
        '''
        Does what it says, trims the fat and removes weird characters/unnecessary words appended to unique manufacturer names
        ''' 
        lowercase_mfg = str(some_mfg).lower()
        
        for spec_char in list("[@_!#$%^&*()<>?/\|}{~:].,-'"):
            lowercase_mfg = lowercase_mfg.replace(spec_char,"")
        
        for fluff in [' manufacturing',' professional',' appliance',' foodservice', ' industries',' company',' mfg',
                      ' refrigeration', ' commercial', ' systems', ' products',' equipment', ' foods',
                      ' international', ' water heater', ' technologies', ' ovens',' range']:
            
            #False positives for any company name w/ "American" in it were common, so excluding them explicitly here
            if "american" not in lowercase_mfg:
                lowercase_mfg = lowercase_mfg.replace(fluff,"")
        
        #Have to do this bit separate because might accidentally scrub other parts of strings
        if (lowercase_mfg[-3:] == "llc") or (lowercase_mfg[-3:] == "inc") or (lowercase_mfg[-2:] == "co"):
            lowercase_mfg = lowercase_mfg.replace(" llc","").replace(" inc","").replace(" co","")
        
        return (some_mfg,lowercase_mfg)



    #Maybe use this later for parts too
    def model_fluff_scrubber(some_model):
        '''
        Does what it says, trims the fat and removes weird characters
        ''' 
        lowercase_model = str(some_model).lower()
        lowercase_model = lowercase_model.replace("series","").strip()
        
        ###To catch manual error for similar characters (maybe incorporate later. Might cause false positives or throw off real matches)
        #lowercase_model = lowercase_model.replace("8","B").replace("O","0").replace("I","1")
        #lowercase_model = lowercase_model.replace("B","8").replace("0","O").replace("1","I")
        
        for spec_char in list("[@_!#$%^&*()<>?/\|}{~:].,-' "): #Dunno whether to include a space here, hmmm
            lowercase_model = lowercase_model.replace(spec_char,"")
        
        return lowercase_model



    def max_value_grabber(matchlist):
        best_match_num = 1
        best_match = ''

        for x in matchlist:
            if x[5] >= best_match_num:
                best_match_num = x[5]
                best_match = x
        
        return best_match



    # File download button (will appear after processing)
    def convert_df_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        output.seek(0)  # Move the pointer to the beginning of the stream
        return output



    ### App name (displays in large text at top of app)
    #st.title("Model and Part Matching")

    ### App instructions
    #st.subheader('This app will accept any .csv or .xlsx file.')
    #st.subheader('Please ensure the column with manufacturer listed is called "Customer_Mfg_Name", and the column with models is called "Customer_Model_Name".')


    # File uploader widget
    customer_file = st.file_uploader("Upload Your File", type=["csv", "xlsx"])

    ### Check if file is loaded (wasn't necessary to print, uploader shows file once it's loaded anyway)
    #if customer_file is not None:
        # Display file details
        #st.write(f"Uploaded file: {customer_file.name}")



    current_date = datetime.datetime.now().strftime("%Y-%m-%d")


    # Main execution block
    if st.button("Begin Model Matching"):
        try:
            # Reading in Pimberly manufacturer list
            pt_mfg_export_df = pd.read_csv("https://github.com/eashcraft/Streamlit-Public-App-Testing/blob/main/EverythingAppTest/pimberly_manufacturer_list.csv", delimiter=",", quoting=3, engine="python")
            #pt_mfg_export_df = pt_mfg_export_df[pt_mfg_export_df["HIDEMANUFACTURER"] != "Y"]
            
            #    Exporting Mfg Names to a list, attaching original MFG names to scrubbed strings
            pt_mfg_list = pt_mfg_export_df["Name"].to_list()
            pt_mfg_match_list = [mfg_fluff_scrubber(m) for m in pt_mfg_list]
            

            #    Getting customer info, in whatever format it may be
            if ".xlsx" in customer_file.name:
                customer_df = pd.read_excel(customer_file, engine="openpyxl")
            elif ".csv" in customer_file.name:
                customer_df = pd.read_csv(customer_file)
            

            
            
            #     Letting user know that app has moved on to the model matching section
            st.write(f"Initiating Manufacturer Matching...")
            

            #    To remove possibility of duplicates later on because of unnecessary spaces
            customer_df["Customer_Mfg_Name"] = customer_df["Customer_Mfg_Name"].apply(lambda x: str(x).strip())
            
            dedupe_customer_mfgs = customer_df["Customer_Mfg_Name"].to_list()
            dedupe_customer_mfgs = list(set(dedupe_customer_mfgs))
            
            customer_mfg_match_list = [mfg_fluff_scrubber(n) for n in dedupe_customer_mfgs]
            

            #    ## Matching MFG Section ###
            clean_mfg_matches = []
            
            for customer_mfg in customer_mfg_match_list:
                scrubbed_customer_mfg = customer_mfg[1]
                
                for pt_mfg in pt_mfg_match_list:
                    scrubbed_pt_mfg = pt_mfg[1]
                    
                    #Tests match percentage & also makes sure letter of first names are same to scrub out some common false positives
                    mfg_match_percentage = fuzz.ratio(scrubbed_pt_mfg,scrubbed_customer_mfg)
                    if (mfg_match_percentage >= 90) and (scrubbed_customer_mfg[0] == scrubbed_pt_mfg[0]):
                        
                        #print(f"Clean Matched {pt_mfg[0]} with {customer_mfg[0]}")
                        clean_mfg_matches.append([customer_mfg[0],pt_mfg[0]])
                    else:
                        #Looking for partial matches
                        partial_match_percentage = fuzz.token_set_ratio(scrubbed_pt_mfg,scrubbed_customer_mfg)
                        if (partial_match_percentage == 100): #and (scrubbed_customer_mfg[0] == scrubbed_pt_mfg[0]):
                            #print(f"Partial Matched {pt_mfg[0]} with {customer_mfg[0]}")
                            clean_mfg_matches.append([customer_mfg[0],pt_mfg[0]])
            

            #    Turning MFGs matches into DataFrame, adding back on to original df
            clean_mfg_match_df = pd.DataFrame(clean_mfg_matches,columns = ["Original_Customer_Mfg_Name","PT_Mfg_Name"])
            
            proto_report_df = pd.merge(left=customer_df, right=clean_mfg_match_df, how="left", left_on = 'Customer_Mfg_Name', right_on = 'Original_Customer_Mfg_Name', validate="many_to_many", sort = True)
            proto_report_df["PT_Mfg_Name"] = proto_report_df["PT_Mfg_Name"].apply(lambda x: str(x).replace("nan","No Manufacturer Match"))
            proto_report_df.drop(["Original_Customer_Mfg_Name"],axis=1,inplace=True)
            

            
            
            #     Removing invalid manufacturer matches, reducing DataFrame to only matched manufacturer names then dropping duplicates
            mfg_code_df = proto_report_df[proto_report_df['PT_Mfg_Name'] != 'No Manufacturer Match']
            mfg_code_df = mfg_code_df[['PT_Mfg_Name']].drop_duplicates()
            

            #     Merging new DataFrame original Mfg Dataset to pull Codes & Parent Mfgs
            merged_mfg_code_df = pd.merge(left=mfg_code_df, right=pt_mfg_export_df, how="left", left_on = 'PT_Mfg_Name',
                     right_on = 'Name', validate="one_to_many")
            
            merged_mfg_code_df = merged_mfg_code_df[['Name','Code','Parent Manufacturer']]
            
            #     Reducing original Mfg Dataset to only Mfgs with a valid Parent (otherwise next merge will merge on null values)
            parent_mfg_df = pt_mfg_export_df[['Name','Code','Parent Manufacturer']]
            parent_mfg_df = parent_mfg_df.dropna(subset = ['Parent Manufacturer'])
            

            #     Merging DataFrames on Parent Mfg so I don't miss cross-branded items (only applying to part output for now, might do for models in the future)
            merged_parent_code_df = pd.merge(left=merged_mfg_code_df, right=parent_mfg_df, left_on='Parent Manufacturer', right_on='Parent Manufacturer',
                                            how='left', suffixes=('', '_Secondary'))
            
            #     Editing output a bit to remove duplicates/rename columns etc. so it can be concatenated with other DataFrame
            merged_parent_code_df = merged_parent_code_df[['Name','Code_Secondary']].drop_duplicates()
            
            #     Some null values were appearing in Code_Secondary column of this DataFrame, not sure why. This scrubs them out
            merged_parent_code_df = merged_parent_code_df.dropna(subset = ['Code_Secondary'])
            
            merged_parent_code_df.rename(columns={"Code_Secondary": "Code"},inplace=True)
            
            merged_mfg_code_df.drop(["Parent Manufacturer"],axis=1,inplace=True)
            
            #    ## Finalizing DataFrame I'll use to feed Mfg Codes through Pimberly API, later match up parts with valid cross-brands too
            
            #    ## Removed this for the time being. Querying every single sister brand for parts put just the first section of API queries at close to an hour
            #clean_mfg_code_df = pd.concat([merged_mfg_code_df,merged_parent_code_df])
            #clean_mfg_code_df = clean_mfg_code_df.drop_duplicates()
            
            clean_mfg_code_df = merged_mfg_code_df.drop_duplicates()
            

            mfg_code_feed_list = clean_mfg_code_df['Code'].to_list()
            mfg_code_feed_list = list(set(mfg_code_feed_list))
            

            
            #    ## API Query section (grabs data to build part & model DataFrames for matching) ###
            
            ###     If query is interrupted, Streamlit will cache data with the below so you can pick up where you left off (never mind, this was throwing error)
            #@st.cache_data
            
            #     Define list to store primary IDs
            prim_id_list = []
            
            #     Base API URL without query parameters
            base_url = "https://app.us.pimberly.com/api/v2.2/products"
            
            #     Define headers with authorization
            headers = {
                'Authorization': 'VTkxRkIVRkwIBdjaMFfXXKnD01LHE4APUFBN7DKltTUowG4HMUiInvUwNOzu1roh'
            }
            

            
            #     Letting user know that app has moved on to the model matching section
            st.write(f"Initiating Pimberly API Queries (hold tight, this can take a while)...")
            
            #     Progress bar (to track how long this takes)
            progress = st.progress(0)
            

            for manufacturer_code in mfg_code_feed_list:
                params = {"filters": f'{{"manufacturerCode":"{manufacturer_code}"}}'}
            

                    # Function to fetch data from a given URL with query parameters
                def fetch_data(url, params):
                    response = requests.get(url, headers=headers, params=params)
                    response.raise_for_status()  # Raise an error if the request fails
                    return response.json()
            
                # Fetch initial data
                json_result = fetch_data(base_url, params)
            
                # Extract/store primary IDs
                product_id_result = json_result['data']
                prim_id_list.extend([p['primaryId'] for p in product_id_result])
            
                # Check if there's a next link to continue scrolling through pages
                next_link = json_result.get('next')
            
                # Cycle through "next" links to get the full product list
                while next_link:
                    try:
                        # For pagination requests, set params to None to avoid duplicate filters
                        json_result = fetch_data(next_link, params=params)
                
                    # Breaks while loop after you've fetched all products. Verified by doing same query via Pimberly UI that number of results is same here
                    except HTTPError as e:
                        break
            
                    product_id_result = json_result['data']
                    prim_id_list.extend([p['primaryId'] for p in product_id_result])
            
                    next_link = json_result.get('next')
            
                    prim_id_list = list(set(prim_id_list))  # Ensure unique IDs
                    #print(f"Total unique IDs collected: {len(prim_id_list)}")
            

            #     Slashes break API queries. Accounting for that below
            prim_id_list = [p.replace("/","%2F") for p in prim_id_list]
            

            #     Previous API requests only return product ID. This converts it to URL I need to pass in later
            p_id_list = [f"https://app.us.pimberly.com/api/v2.2/products/{p}/attributes" for p in prim_id_list]
            
            #     Saving all output data to this DataFrame
            master_attr_df = pd.DataFrame()
            
            #     Define the maximum number of concurrent requests. Makes sure we don't send too many requests at once
            MAX_CONCURRENT_REQUESTS = 15
            

            #     Define the semaphore for controlling concurrent requests
            semaphore = asyncio.Semaphore(max_concurrent_requests)
            

            #     Define the asynchronous function to fetch data from an API
            async def fetch(session, url, headers, progress_tracker):
                async with semaphore:
                    try:
                        async with session.get(url, headers=headers) as response:
                            response.raise_for_status()
                            progress_tracker["completed"] += 1
                            progress.progress(progress_tracker["completed"] / progress_tracker["total"])
                            return await response.json()
                    except Exception as e:
                        st.error(f"Error fetching URL {url}: {e}")
                        return None
            

            #     Define the main function to run multiple asynchronous requests
            async def fetch_all(urls, headers):
                results = []
                progress_tracker = {"completed": 0, "total": len(urls)}
                async with aiohttp.ClientSession() as session:
                    tasks = [fetch(session, url, headers, progress_tracker) for url in urls]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                return [r for r in results if r is not None]
            

            #     Use asyncio with new_event_loop in Streamlit
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = loop.run_until_complete(fetch_all(p_id_list, headers))
            finally:
                loop.close()
            

            #     Convert results to Pandas DataFrame
            result_df = pd.DataFrame.from_dict(results)
            
            #     Timing how long this actually took
            end_time = datetime.datetime.now()
            execution_time = end_time - start_time
            st.success(f"Execution time for API queries: {execution_time}")
            

            #     Filtering out models from the output df so I only have parts (Tilde negates boolean series)
            part_result_df = result_df[~result_df["primaryId"].str.contains("PT_CAT", na=False)]
            
            #     Filtering out parts from the output df so I only have models
            model_result_df = result_df[result_df["primaryId"].str.contains("PT_CAT", na=False)]
            

            #     Narrowing down columns
            part_result_df = part_result_df[['primaryId','manufacturerPartNumber','title','optimizedDesc','marketplacePart','obsolete','manufacturerName']]
            model_result_df = model_result_df[['primaryId','manufacturerPartNumber','optimizedDesc','manufacturerName']]
            

            #     Exporting all relevant models to list for matching process
            pce_ids = model_result_df["primaryId"].to_list()
            pce_names = model_result_df["manufacturerPartNumber"].to_list()
            pce_mfgs = model_result_df["manufacturerName"].to_list()
            
            full_pt_model_list = list(zip(pce_ids,pce_names,pce_mfgs))
            

            
            #     Letting user know that app has moved on to the model matching section
            st.write(f"Initiating Model Matching...")
            

            clean_model_matches = []
            
            #    Adding cleaned PT MFGs to customer models, exporting to list
            matched_model_mfgs = proto_report_df["PT_Mfg_Name"].to_list()
            proto_customer_model_list = proto_report_df["Customer_Model_Name"].to_list()
            customer_model_list = list(zip(matched_model_mfgs,proto_customer_model_list))
            customer_model_list = list(set(customer_model_list))
            

            #    Cycling through and actually matching up the models
            for customer_model in customer_model_list:
                #Renaming sections of tuples to make rest of the script more intutive
                matched_mfg_name = customer_model[0]
                customer_model_name = str(customer_model[1])
                
                this_model_matches = []
                
                for pt_model in full_pt_model_list:
                    pt_model_name = str(pt_model[1])
                    
                    #Making sure MFGs match (str() on 2nd one because True autoreturns as a Boolean otherwise)
                    if matched_mfg_name == str(pt_model[2]):
                        
                        #Getting ratio here so can determine whether to use fuzz.ratio (elif clause) or fuzz.token_set_ratio (else clause)
                        model_match_percentage = fuzz.ratio(customer_model_name,pt_model_name)
                        partial_match_percentage_one_way = fuzz.token_set_ratio(customer_model_name,pt_model_name)
                        partial_match_percentage_other_way = fuzz.token_set_ratio(pt_model_name,customer_model_name) #.replace(" Single","").replace("Double", "").replace("Triple",""))
                        
                        if model_fluff_scrubber(customer_model_name) == model_fluff_scrubber(pt_model_name):
                            #print(f"Exact Match for {customer_model} with {pt_model}")
                            
                            clean_model_match_entry = list(customer_model + pt_model) + [101]
                            this_model_matches.append(clean_model_match_entry)
                            
                            
                        if (model_match_percentage >= 84) and (customer_model_name[0] == pt_model_name[0]) and (customer_model_name[0:2] == pt_model_name[0:2]):
                            #print(f"Clean Match for {customer_model} with {pt_model} at {model_match_percentage}%")
                            
                            clean_model_match_entry = list(customer_model + pt_model) + [model_match_percentage]
                            this_model_matches.append(clean_model_match_entry)
            
                        
                        elif(partial_match_percentage_one_way >= 95) and (len(pt_model_name) >= 3) and (customer_model_name[0:2] == pt_model_name[0:2]):
                            #Looking for partial matches
                            #print(f"Partial Match for {customer_model} with {pt_model} at {partial_match_percentage_one_way-15}%")
                                
                            #Knocking 15 off the rank because partial 100 is treated same as exact or high clean value
                            clean_model_match_entry = list(customer_model + pt_model) + [partial_match_percentage_one_way - 15]
                            this_model_matches.append(clean_model_match_entry)
                        
                        elif(partial_match_percentage_other_way >= 95) and (len(pt_model_name) >= 3) and (customer_model_name[0:2] == pt_model_name[0:2]):
                            #Looking for partial matches
                            #print(f"Partial Match for {customer_model} with {pt_model} at {partial_match_percentage_other_way-15}%")
                            
                            #Knocking 15 off the rank because partial 100 is treated same as exact or high clean value
                            clean_model_match_entry = list(customer_model + pt_model) + [partial_match_percentage_other_way - 15]
                            this_model_matches.append(clean_model_match_entry)
            
                        #Added 11-16-2022
                        elif (model_fluff_scrubber(pt_model_name) in model_fluff_scrubber(customer_model_name)) and (len(pt_model_name) > 2) and (model_fluff_scrubber(pt_model_name)[0] == model_fluff_scrubber(customer_model_name)[0]):
                            #print(f"Base Model Match for {customer_model} with {pt_model}")
                            
                            clean_model_match_entry = list(customer_model + pt_model) + [75]
                            this_model_matches.append(clean_model_match_entry)
            

                 #Vetting the specific matches for this model to only keep exact if there's a hit on that type
                #There's only one possible exact match so as soon as I find it, I can turn the list into it
                if this_model_matches != []:
                    for rank in this_model_matches:
                        if rank[5] == 101:
                            #Doing [rank] here because the multiple matches spit out as lists of lists. Want consistent formatting
                            this_model_matches = [rank]
                            #print("Scrubbed all but exact from the test matches") #If this message repeats mult. times it's because clean match percentage was 100 too
                        
                    #Removes match duplicates, only keeps one with highest percentage
                    this_model_matches = max_value_grabber(this_model_matches)
                    
                    #Only printing this under the IF clause above because otherwise empty lists will print
                    #print(this_model_matches)
                    clean_model_matches.append(this_model_matches)
            

            #     Turning matches into clean dataframe, removing duplicates, spitting out final file
            model_match_df = pd.DataFrame(clean_model_matches,columns = ["PT_Mfg_Name","Customer_Model_Name",
                                        "PT_Category_ID", "PT_Model_Name", "Redundant_Mfg_Name","Model_Match_Quality"])
            

            model_match_df.drop(["Redundant_Mfg_Name"],axis=1,inplace=True)
            

            #    Keeping manufacturer in there so that can match up names, remove the incorrectly associated names
            final_df = pd.merge(left=proto_report_df, right=model_match_df, how= 'left',left_on= ['Customer_Model_Name','PT_Mfg_Name']
                ,right_on=['Customer_Model_Name','PT_Mfg_Name'],validate='many_to_many',sort = False)
            
            #    final_df.drop(["PT_Mfg_Name_y"],axis=1,inplace=True)
            #    final_df.rename(columns={"PT_Mfg_Name_x": "PT_Mfg_Match", "PT_Model_Name": "PT_Model_Match"},inplace=True)
            final_df.rename(columns={"PT_Model_Name": "PT_Model_Match"},inplace=True)
            
            final_df["PT_Category_ID"] = final_df["PT_Category_ID"].fillna("No Model Match")
            final_df["PT_Model_Match"] = final_df["PT_Model_Match"].fillna("No Model Match")
            

            #    Removes duplicate manufacturer matches (if you want to retain multiple potential manufacturer matches, then mute the following 3 lines of code)
            final_df['Customer_Model_Name'] = final_df['Customer_Model_Name'].apply(str)
            final_df = final_df.sort_values(by = ['PT_Category_ID'], ascending=False)
            final_df.drop_duplicates(subset = ['Customer_Model_Name'],keep = "first",inplace=True)
            

            
            #     Letting user know that app has moved on to the model matching section
            st.write(f"Initiating Part Matching...")
            

            #     Exporting all relevant models to list for matching process
            pr_ids = part_result_df["primaryId"].to_list()
            pr_names = part_result_df["manufacturerPartNumber"].to_list()
            pr_mfgs = part_result_df["manufacturerName"].to_list()
            
            full_part_result_list = list(zip(pr_ids,pr_names,pr_mfgs))
            

            
            clean_part_matches = []
            
            #    Adding cleaned PT MFGs to customer models, exporting to list
            matched_part_mfgs = proto_report_df["PT_Mfg_Name"].to_list()
            proto_customer_part_list = proto_report_df["Customer_Model_Name"].to_list()
            customer_part_list = list(zip(matched_part_mfgs,proto_customer_part_list))
            customer_part_list = list(set(customer_part_list))
            

            #    Cycling through and actually matching up the models
            for customer_part in customer_part_list:
                #Renaming sections of tuples to make rest of the script more intutive
                matched_mfg_name = customer_part[0]
                customer_part_name = str(customer_part[1])
                
                for pt_part in full_part_result_list:
                    pt_part_name = str(pt_part[1])
                    
                    #Making sure MFGs match (str() on 2nd one because True autoreturns as a Boolean otherwise)
                    if matched_mfg_name == str(pt_part[2]):
                        
                        if all_purpose_cleaner(customer_part_name) == all_purpose_cleaner(pt_part_name):
                            #print(f"Exact Match for {customer_part} with {pt_part}")
                            
                            clean_part_match_entry = list(customer_part + pt_part)
                            clean_part_matches.append(clean_part_match_entry)
            

            
            part_match_df = pd.DataFrame(clean_part_matches,columns = ["PT_Mfg_Name","Customer_Part_Number",
                                        "PT_Part_Number", "PT_MFG_Part_Number", "Redundant_Mfg_Name"])
            
            part_match_df.drop(["Redundant_Mfg_Name"],axis=1,inplace=True)
            

            #    Keeping manufacturer in there so that can match up names, remove the incorrectly associated names
            final_df = pd.merge(left=proto_report_df, right=part_match_df, how= 'left',left_on= ['Customer_Model_Name','PT_Mfg_Name']
                ,right_on=['Customer_Part_Number','PT_Mfg_Name'],validate='many_to_many',sort = False)
            
            final_df.drop(['Customer_Part_Number'],axis=1,inplace=True)
            

            
            
            #     Final file output
            final_filename_chunk = customer_file.replace(".csv","").replace(".xlsx","").replace(" ","_").replace("__","_")
            #final_df.to_excel(f"{final_filename_chunk} Equipment Audit {current_date}.xlsx", index=False)
            

            #     Convert link dict df to a csv
            excel_data = convert_df_to_excel(final_df)
            

            #     Creating download button for main part data
            st.download_button(
                label="Download Match File",
                data=excel_data,
                file_name=f"{final_filename_chunk}_Equipment_Part_Audit_{current_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            st.error(f"An error occurred: {e}")
