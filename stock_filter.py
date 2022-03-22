"""The functions in file parse stock records, process and export in tabular format to a tsv file."""
import os
import csv
import json
import logging
import re
import pandas as pd
from itertools import groupby
from operator import itemgetter
from collections import defaultdict


logger = logging.getLogger(__name__)


def parse_file_records(filename):
    """Parse the contents of stock file records."""
    try:
        with open(filename, 'r') as f:
            lst = f.read().splitlines()
        return lst
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def get_file_records_8(stock_record_lst):
    """Fetch the contents of stock file records for msgType_:8."""
    try:
        lst, lst_dict_8 = [x for x in stock_record_lst if '"msgType_":8' in x], []
        for item in lst:
            match = re.search('securityId_', item)
            match1 = re.search('flags_', item)

            result = item.index(match.group()) - 2
            result1 = item.index(match1.group()) - 2
            # Get the json object from index of result to result1
            lst_dict_8.append(json.loads(item[result:result1] + '}'))
        return lst_dict_8
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def get_file_records_12(stock_record_lst):
    """Fetch the contents of stock file records for msgType_:12."""
    try:
        lst, lst_dict_12 = [x for x in stock_record_lst if '"msgType_":12' in x], []

        for item in lst:
            match = re.search('securityId_', item)

            match1 = re.search('BUY', item)
            match2 = re.search('SELL', item)
            result = item.index(match.group()) - 2
            if match1:
                # Get the json object from index of result to result5 and convert 'BUY' to "BUY", add from former
                result5 = item.index(match1.group())
                lst_dict_12.append(json.loads(item[result:result5] + '"BUY",' + item[result5 + 4:-1]))

            elif match2:
                match2 = re.search('SELL', item)
                result6 = item.index(match2.group())
                # Get the json object from index of result to result6 and convert 'BUY' to "BUY", add from former
                lst_dict_12.append(json.loads(item[result:result6] + '"SELL",' + item[result6 + 5:-1]))

        return lst_dict_12
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def get_filtered_records(stock_record_lst):
    """Get the resultant list of dictionaries in expected format."""
    try:
        filtered_joined_lst = []
        lst_dict_12 = get_file_records_12(stock_record_lst)
        lst_dict_8 = get_file_records_8(stock_record_lst)

        for item in lst_dict_8:
            filtered_joined_lst.extend([dict(zip(['ISIN', 'Currency', 'side_', 'quantity_', 'price_', 'price_per_unit'],
                                                 [item['isin_'], item['currency_'], x['side_'], x['quantity_'],
                                                  x['price_'], x['price_'] / x['quantity_']]))
                                        for x in lst_dict_12 if x['securityId_'] == item['securityId_']])

        resultant_aggregated_lst = get_resultant_lst(filtered_joined_lst)
        result_count = get_total_buy_sell_count(filtered_joined_lst)
        result_max_min = get_max_min_buy_sell_price(filtered_joined_lst)
        # Get the list of dictionaries for minimum sale price
        comp_min_sell_price = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                                'Min Sell Price': d['Min Sell Price']} for d in result_max_min if d['side_'] == 'SELL']
        # Get the list of dictionaries for buy count
        comp_buy_count = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                           'Total Buy Count': d['Total Buy Count']} for d in result_count if d['side_'] == 'BUY']

        # Get the list of dictionaries for sale count
        comp_sell_count = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                            'Total Sell Count': d['Total Sell Count']} for d in result_count if d['side_'] == 'SELL']

        keylist = ['ISIN', 'Currency', 'Total Buy Quantity', 'Total Sell Quantity', 'Weighted Average Buy Price',
                   'Weighted Average Sell Price', 'Max Buy Price']
        keys_lst = ['price_', 'quantity_', 'weighted_average', 'price_per_unit']

        for item in resultant_aggregated_lst:
            if item['side_'] == 'BUY':
                item[keylist[2]], item[keylist[4]], item[keylist[6]] = item['quantity_'], \
                                                                       item['weighted_average'], \
                                                                       item['price_per_unit']
                for key in keys_lst:
                    del item[key]

            elif item['side_'] == 'SELL':
                item[keylist[3]], item[keylist[5]] = item['quantity_'], item['weighted_average']

                for key in keys_lst:
                    del item[key]
        # Create composite key for keys ISIN, Currency and side_
        for dic in resultant_aggregated_lst:
            dic['ISINCurrencyside_'] = f"{dic['ISIN']}|{dic['Currency']}|{dic['side_']}"
            del dic['ISIN']
            del dic['Currency']
            del dic['side_']

        default_dic = defaultdict(dict)
        # Combine the processed list of dictionaries
        for item in resultant_aggregated_lst + comp_min_sell_price + comp_buy_count + comp_sell_count:
            default_dic[item['ISINCurrencyside_']].update(item)
        res_lst = list(default_dic.values())

        for d in res_lst:
            key_list = ['ISINCurrency']
            if 'BUY' in d['ISINCurrencyside_']:
                d['ISINCurrencyside_'] = d['ISINCurrencyside_'][:-4]
                d[key_list[0]] = d['ISINCurrencyside_']
                del d['ISINCurrencyside_']
            elif 'SELL' in d['ISINCurrencyside_']:
                d['ISINCurrencyside_'] = d['ISINCurrencyside_'][:-5]
                d[key_list[0]] = d['ISINCurrencyside_']
                del d['ISINCurrencyside_']
        # Keep the key as 'ISINCurrency' to avoid duplication of rows for same ISIN and Currency
        default_dict = defaultdict(dict)
        for item in res_lst:
            default_dict[item['ISINCurrency']].update(item)
        resultant_isin_curr_lst = list(default_dict.values())

        # Split the key 'ISINCurrency' and remove composite key
        for d in resultant_isin_curr_lst:
            key_list = ['ISIN', 'Currency']
            d['ISINCurrency'] = d['ISINCurrency'].split('|')
            d[key_list[0]] = d['ISINCurrency'][0]
            d[key_list[1]] = d['ISINCurrency'][1]

            del d['ISINCurrency']

        return resultant_isin_curr_lst
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ', None]))
        return output_json


def get_resultant_lst(lst):
    """Get the resultant aggregation of processes stock records."""
    try:
        df = pd.DataFrame(lst)
        # Get the date frame for quantity
        df_quantity = df[['ISIN', 'Currency', 'side_', 'quantity_']].groupby(['ISIN', 'Currency', 'side_']) \
            .sum('quantity_').apply(list).reset_index()
        # df_buy_sale_count = df[['ISIN', 'Currency','side_', 'price_']].groupby(['ISIN','Currency','side_'])['price_']
        # .count().reset_index(name='count')
        # Get the date frame for max buy and sale price
        df_max_buy_sale_price = df[['ISIN', 'Currency', 'side_', 'price_per_unit']]. \
            groupby(['ISIN', 'Currency', 'side_']) \
            .max('price_per_unit').apply(list).reset_index()
        # Get the date frame for price
        df_price = df[['ISIN', 'Currency', 'side_', 'price_']].groupby(['ISIN', 'Currency', 'side_']) \
            .sum('price_').apply(list).reset_index()
        # final_data = df[['ISIN', 'Currency','side_', 'price_per_unit']].groupby(['ISIN','Currency','side_'])
        # .agg({"price_per_unit": ["max", "min"]}).apply(list).reset_index()

        # Concatenate the data frames
        df_concat = pd.concat([df_quantity, df_price, df_max_buy_sale_price], axis='columns')

        df = df_concat
        df2 = df.loc[:, ~df.columns.duplicated()]
        # Get the weighted average for date frames
        df2['weighted_average'] = df2['price_'] / df2['quantity_']

        lst_dicts = df2.to_dict('r')
        return lst_dicts
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ', None]))
        return output_json


def get_total_buy_sell_count(lst):
    """Get the total buy count and total sell count for selected stock records."""
    try:
        grouper = itemgetter('ISIN', 'Currency', 'side_')
        result_count = []
        for key, grp in groupby(sorted(lst, key=grouper), grouper):
            temp_dict = dict(zip(["ISIN", "Currency", "side_"], key))

            if temp_dict['side_'] == 'BUY':
                temp_list = [item["price_"] for item in grp]
                temp_dict["Total Buy Count"] = len(temp_list)

            else:
                temp_list = [item["price_"] for item in grp]
                temp_dict["Total Sell Count"] = len(temp_list)

            result_count.append(temp_dict)

        return result_count
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def get_max_min_buy_sell_price(lst):
    """Get the Maximum buy price and Minimum sell price for selected stock records."""
    try:
        grouper = itemgetter('ISIN', 'Currency', 'side_')
        result = []
        for key, grp in groupby(sorted(lst, key=grouper), grouper):
            temp_dict = dict(zip(['ISIN', 'Currency', 'side_'], key))
            if temp_dict['side_'] == 'BUY':
                temp_list = [item["price_per_unit"] for item in grp]
                temp_dict["Max Buy Price"] = max(temp_list)
                # temp_dict["Min_Buy_Price"] = min(temp_list)
            else:
                temp_list = [item["price_per_unit"] for item in grp]
                # temp_dict["Max_Sell_Price"] = max(temp_list)
                temp_dict["Min Sell Price"] = min(temp_list)
            result.append(temp_dict)
        return result
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ', None]))
        return output_json


def export_to_tsv(res_lst, tsv_file):
    """Export the resultant list of dictionaries in tabular format."""
    try:
        dir_name = os.path.dirname(os.path.abspath(__file__))

        keys = ['ISIN', 'Currency', 'Total Buy Count', 'Total Sell Count', 'Total Buy Quantity', 'Total Sell Quantity',
                'Weighted Average Buy Price', 'Weighted Average Sell Price', 'Max Buy Price', 'Min Sell Price']

        csv_filename = os.path.join(dir_name, tsv_file)

        with open(csv_filename, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys, dialect='excel-tab')
            dict_writer.writeheader()
            dict_writer.writerows(res_lst)
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def master_process_records(filename, tsv_file):
    """Execute this function to execute processing of stock records and export results to tsv file"""
    dir_name = os.path.dirname(os.path.abspath(__file__))
    stock_filename = os.path.join(dir_name, filename)
    stock_records_lst = parse_file_records(stock_filename)
    filtered_lst = get_filtered_records(stock_records_lst)
    export_to_tsv(filtered_lst, tsv_file)


if __name__ == '__main__':
    """Call master function to execute processing of stock records and export results to tsv file."""
    master_process_records('stock_records', 'TSVFile.tsv')
