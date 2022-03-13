#####################################
Problem Statement:
__________________
Exchange transactions are available in the form of a text file with each transaction as a stringified version of a json object.
We need to read this file, identify the records that signify any particular security and records that signify transactions on any given security.
Refine the data to extract information in a usable fashion.
Analyze and process the refined data to identify  Total Sell Count, Total Sell Quantity, Max Buy Price, Min Sell Price, and Weighted Average Sell Price.

#####################################

#####################################
Sample Input:
_______________
0 {{"msgType_":8,"length_":36,"seqNo_":460},"security_":{"securityId_":3026,"umtf_":"AMSz","isin_":"AT0000A18XM4","currency_":"CHF","mic_":"XSWX","tickTableId_":34,"flags_":"{"b_":{"closingEnabled_":1,"testStock_":0,"illiquid":0,"live_":1,"aodEnabled_":1},"v_":25}}}
0 {{"msgType_":8,"length_":36,"seqNo_":461},"security_":{"securityId_":3025,"umtf_":"DUFNz","isin_":"CH0023405456","currency_":"CHF","mic_":"XSWX","tickTableId_":34,"flags_":"{"b_":{"closingEnabled_":1,"testStock_":0,"illiquid":0,"live_":1,"aodEnabled_":1},"v_":25}}}
0 {{"msgType_":8,"length_":36,"seqNo_":462},"security_":{"securityId_":3022,"umtf_":"TUI1d","isin_":"DE000TUAG000","currency_":"EUR","mic_":"XETR","tickTableId_":34,"flags_":"{"b_":{"closingEnabled_":0,"testStock_":0,"illiquid":0,"live_":1,"aodEnabled_":0},"v_":8}}}
0 {{"msgType_":8,"length_":36,"seqNo_":949},"security_":{"securityId_":1347,"umtf_":"BLSz","isin_":"CH0460027110","currency_":"CHF","mic_":"XSWX","tickTableId_":30,"flags_":"{"b_":{"closingEnabled_":1,"testStock_":0,"illiquid":0,"live_":1,"aodEnabled_":1},"v_":25}}}
0 {{"msgType_":11,"length_":28,"seqNo_":16780},"bookStatus_":{"securityId_":3026,"tradingStatus_":1,"marketFlags_":17,"entries_":9,"closingBuyQty_":0,"closingSellQty_":0,"indicativePrice_":0}}
0 {{"msgType_":12,"length_":25,"seqNo_":16781},"bookEntry_":{"securityId_":3026,"side_":BUY,"quantity_":150,"price_":1000000,"orderId_":56914}}
0 {{"msgType_":12,"length_":25,"seqNo_":16782},"bookEntry_":{"securityId_":3026,"side_":BUY,"quantity_":429,"price_":1290000,"orderId_":424607}}
0 {{"msgType_":12,"length_":25,"seqNo_":16783},"bookEntry_":{"securityId_":3026,"side_":SELL,"quantity_":600,"price_":1301500,"orderId_":538981}}
0 {{"msgType_":12,"length_":25,"seqNo_":16784},"bookEntry_":{"securityId_":3026,"side_":SELL,"quantity_":429,"price_":1312000,"orderId_":507862}}
#####################################


#####################################
Expected output:
_______________
{
['ISIN':'AT0000A18XM4', 'Currency': 'CHF', 'Total Buy Count': 168, 'Total Buy Quantity': 90243, 'Max Buy Price': 83200.0, 'Weighted Average Buy Price': 2211.6119809846746, 'Total Sell Count': 319, 'Total Sell Quantity': 197561, 'Min Sell Price': 944.636678200692, 'Weighted Average Sell Price': 2247.154043561229},
 {'ISIN': 'AT0000A21KS2', 'Currency': 'EUR', 'Total Buy Count': 17, 'Total Buy Quantity': 28700, 'Max Buy Price': 2288.0, 'Weighted Average Buy Price': 1354.355400696864}, 
 {'ISIN': 'AT0000KTMI02', 'Currency': 'CHF', 'Total Buy Count': 179, 'Total Buy Quantity': 11469, 'Max Buy Price': 133333.33333333334, 'Weighted Average Buy Price': 121304.3857354608, 'Total Sell Count': 243,'Total Sell Quantity': 12179, 'Min Sell Price': 105405.4054054054, 'Weighted Average Sell Price': 157803.59635438048}
] 


#####################################


#####################################
Actual output on Sample data in json format (Output saved in TSV File):
________________________________________________________
{
['ISIN':'AT0000A18XM4', 'Currency': 'CHF', 'Total Buy Count': 168, 'Total Buy Quantity': 90243, 'Max Buy Price': 83200.0, 'Weighted Average Buy Price': 2211.6119809846746, 'Total Sell Count': 319, 'Total Sell Quantity': 197561, 'Min Sell Price': 944.636678200692, 'Weighted Average Sell Price': 2247.154043561229},
 {'ISIN': 'AT0000A21KS2', 'Currency': 'EUR', 'Total Buy Count': 17, 'Total Buy Quantity': 28700, 'Max Buy Price': 2288.0, 'Weighted Average Buy Price': 1354.355400696864}, 
 {'ISIN': 'AT0000KTMI02', 'Currency': 'CHF', 'Total Buy Count': 179, 'Total Buy Quantity': 11469, 'Max Buy Price': 133333.33333333334, 'Weighted Average Buy Price': 121304.3857354608, 'Total Sell Count': 243,'Total Sell Quantity': 12179, 'Min Sell Price': 105405.4054054054, 'Weighted Average Sell Price': 157803.59635438048}
] 
#####################################


#####################################
Logic Used:
_______________
Step 1: From the given input file read the data and split each line into a string and add to a master list
Step 2: From the master list filter and populate two lists: one for msg_type = 8 (list name: lst_dict_8) and one for msg_type = 12 (list name: lst_dict_12)
Step 3: Using python's core features collect sub-string from lst_dict_8 and lst_dict_8, and convert them to json
                Step 3.1: Since json.loads is unable to read 'BUY' and 'SELL' as plain string, convert these to "BUY" and "SELL" before converting to json object
Step 4: Borrowing the concept of left outer join from RDBMS, join lst_dict_12 and lst_dict_8 on securityId_ with lst_dict_12 on the left side
                Let's call this list as filtered_joined_lst
Step 5: Run aggregation logic on filtered_joined_list to get resultant_aggregated_list
Step 6: Save data of resultant_aggregated_lst in tsv file


Assumptions:
____________
Assumption 1: Data in input with msg_type != 8 and 12 are irrelevant and to be ignored
Assumption 2: SecurityID_ is the common link between data with msg_type != 8 and 12
Assumption 3: side_ is considered to be indicative of the type of transaction (Buy/Sell)
Assumption 4: price_ in data with msgtype_8 means the price of that particular transaction 
              and is equal to the product of transaction_quantity and price_per_item
Assumption 5: Based on assumption 4, weighted_average_price = sum(price_)/sum(quantity_) for respective transaction type
Assumption 6: Since these seem to be exhange data, values of every item is kept till 10 decimal places instead of rounding to two decimal places

#####################################


#####################################
How to execute the code for live testing:
________________________________________
1) Download the test file from 
        http://email.hired.com/c/eJwtTstuxCAQ-5pwA_EKWQ4c9tLfiCbM0NAmuymQbtWvL5Eq-eCxLY8xjFoDy8GMSVPyEzcYNbfORO5Hg9wui0kaRu_0bbByzYVQxOfO1iDJk8Mok9JoLYACN3lwoDBZleLCtrC2dtTB3Af91gFfZ678OJctR57yRlVUI-jkL6qNawE7_D4f8KrXg57foXxSmxEa9CuepdCjdXYUagWQ5n9JtJ_Gaicz7ZC3OWNQN-kmqRwrocAH0fcqpz7__fKv9j8Y5lCA

        Copy the content of the file and paste in the file named stock_records.txt

2) Create a new virtual environment
        virtualenv testenv
   
3) Activate the newly created virtual environment
        testenv\Scripts\activate     -> for Windows Machine
        Source testenv/bin/activate  -> for iOS
   
4) go to project folder that contains the file named stock_filter.py and requirements.txt
5) install all requirements to the newly created virtual environment
        pip install -r requirements.txt
   
6) in the terminal window of your ide or in your command prompt execute the file by running the below command based on the system/os you are using 
        python stock_filer.py

7) The code will execute, and the output will be saved in TSVFile.tsv

#####################################


