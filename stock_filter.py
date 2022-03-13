"""The functions in file parse stock records, process and export in tabular format to a tsv file."""
import os
import csv
import json
import logging
import re
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


def get_file_records_8(filename):
    """Fetch the contents of stock file records for msgType_:8."""
    try:
        file_records = parse_file_records(filename)
        lst, lst_dict_8 = [x for x in file_records if '"msgType_":8' in x], []
        for item in lst:
            match = re.search('securityId_', item)
            match1 = re.search('flags_', item)

            result = item.index(match.group()) - 2
            result1 = item.index(match1.group()) - 2

            lst_dict_8.append(json.loads(item[result:result1] + '}'))
        return lst_dict_8
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def get_file_records_12(filename):
    """Fetch the contents of stock file records for msgType_:12."""
    try:
        file_records = parse_file_records(filename)
        lst, lst_dict_12 = [x for x in file_records if '"msgType_":12' in x], []

        for item in lst:
            match = re.search('securityId_', item)

            match1 = re.search('BUY', item)
            match2 = re.search('SELL', item)
            result = item.index(match.group()) - 2
            if match1:
                result5 = item.index(match1.group())
                lst_dict_12.append(json.loads(item[result:result5] + '"BUY",' + item[result5 + 4:-1]))

            elif match2:
                match2 = re.search('SELL', item)
                result6 = item.index(match2.group())
                lst_dict_12.append(json.loads(item[result:result6] + '"SELL",' + item[result6 + 5:-1]))

        return lst_dict_12
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def get_filtered_records(filename):
    """Get the resultant list of dictionaries in expected format."""
    try:
        filtered_lst = []
        lst_dict_8 = get_file_records_12(filename)
        lst_dict_12 = get_file_records_8(filename)

        for item in lst_dict_12:
            filtered_lst.extend([dict(zip(['ISIN', 'Currency', 'side_', 'quantity_', 'price_', 'price_per_unit'],
                                          [item['isin_'], item['currency_'], x['side_'], x['quantity_'],
                                           x['price_'], x['price_'] / x['quantity_']]))
                                 for x in lst_dict_8 if x['securityId_'] == item['securityId_']])
        result = get_resultant_lst(filtered_lst)
        return result
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def get_resultant_lst(lst):
    """Get the resultant aggregation of processes stock records."""
    try:
        result_count = get_total_buy_sell_count(lst)
        result_max_min = get_max_min_buy_sell_price(lst)
        result_price = get_total_buy_sale_price(lst)
        result_quantity = get_total_buy_sell_quantity(lst)

        comp_buy_count = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                           'Total Buy Count': d['Total Buy Count']} for d in result_count if d['side_'] == 'BUY']
        comp_sell_count = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                            'Total Sell Count': d['Total Sell Count']} for d in result_count if d['side_'] == 'SELL']

        comp_buy_quantity = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                              'Total Buy Quantity': d['Total Buy Quantity']}
                             for d in result_quantity if d['side_'] == 'BUY']

        comp_sell_quantity = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                               'Total Sell Quantity': d['Total Sell Quantity']} for d in result_quantity
                              if d['side_'] == 'SELL']

        comp_max_buy_price = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                               'Max Buy Price': d['Max Buy Price']} for d in result_max_min if d['side_'] == 'BUY']

        comp_min_sell_price = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                                'Min Sell Price': d['Min Sell Price']} for d in result_max_min if d['side_'] == 'SELL']

        comp_buy_price = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                           'Total Buy Price': d['Total Buy Price']} for d in result_price if d['side_'] == 'BUY']
        comp_sell_price = [{'ISINCurrencyside_': f"{d['ISIN']}|{d['Currency']}|{d['side_']}",
                            'Total Sell Price': d['Total Sell Price']} for d in result_price if d['side_'] == 'SELL']

        d = defaultdict(dict)
        for item in comp_buy_count + comp_sell_count + comp_buy_quantity + comp_sell_quantity + comp_buy_price + \
                    comp_sell_price + comp_max_buy_price + comp_min_sell_price:
            d[item['ISINCurrencyside_']].update(item)
        res_lst = list(d.values())
        for d in res_lst:
            if 'BUY' in d['ISINCurrencyside_']:
                d['Weighted Average Buy Price'] = ((d['Total Buy Price']) / d['Total Buy Quantity'])
            else:
                d['Weighted Average Sell Price'] = ((d['Total Sell Price']) / d['Total Sell Quantity'])
        for d in res_lst:
            if 'BUY' in d['ISINCurrencyside_']:
                del d['Total Buy Price']
            if 'SELL' in d['ISINCurrencyside_']:
                del d['Total Sell Price']

        for d in res_lst:
            key_list = ['ISIN', 'Currency', 'side_']
            d['ISINCurrencyside_'] = d['ISINCurrencyside_'].split('|')
            d[key_list[0]] = d['ISINCurrencyside_'][0]
            d[key_list[1]] = d['ISINCurrencyside_'][1]
            # d[key_list[2]] = d['ISINCurrencyside_'][2]
            del d['ISINCurrencyside_']
        return res_lst
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ', None]))
        return output_json


def get_total_buy_sell_count(lst):
    """Get the total buy count and total sale count for selected stock records."""
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
            # result = result1 + result2
        return result_count
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def get_total_buy_sell_quantity(lst):
    """Get the total buy quantity and total sale quantity for selected stock records."""
    try:
        grouper = itemgetter('ISIN', 'Currency', 'side_')

        result = []
        for key, grp in groupby(sorted(lst, key=grouper), grouper):
            temp_dict = dict(zip(['ISIN', 'Currency', 'side_'], key))

            if temp_dict['side_'] == 'BUY':
                temp_dict["Total Buy Quantity"] = sum(item["quantity_"] for item in grp)
            else:
                # temp_lst = [item["quantity_"] for item in grp]
                temp_dict["Total Sell Quantity"] = sum(item["quantity_"] for item in grp)

            result.append(temp_dict)
        return result
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ', None]))
        return output_json


def get_max_min_buy_sell_price(lst):
    """Get the Maximum buy price and Minimum sale price for selected stock records."""
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


def get_total_buy_sale_price(lst):
    """Get the total buy price and total sale price for selected stock records."""
    try:
        grouper = itemgetter('ISIN', 'Currency', 'side_')
        result = []
        for key, grp in groupby(sorted(lst, key=grouper), grouper):
            temp_dict = dict(zip(['ISIN', 'Currency', 'side_'], key))

            if temp_dict['side_'] == 'BUY':
                temp_list = [item["price_"] for item in grp]
                temp_dict["Total Buy Price"] = sum(temp_list)

            else:
                temp_list = [item["price_"] for item in grp]
                temp_dict["Total Sell Price"] = sum(temp_list)

            result.append(temp_dict)
        return result
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


def export_to_tsv(filename, tsv_file):
    """Export the resultant list of dictionaries in tabular format."""
    try:
        dir_name = os.path.dirname(os.path.abspath(__file__))
        stock_filename = os.path.join(dir_name, filename)
        lst = get_filtered_records(stock_filename)
        keys = ['ISIN', 'Currency', 'Total Buy Count', 'Total Sell Count', 'Total Buy Quantity', 'Total Sell Quantity',
                'Weighted Average Buy Price', 'Weighted Average Sell Price', 'Max Buy Price', 'Min Sell Price']

        csv_filename = os.path.join(dir_name, tsv_file)

        with open(csv_filename, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys, dialect='excel-tab')
            dict_writer.writeheader()
            dict_writer.writerows(lst)
    except Exception as ex:
        output_json = dict(zip(['Message', 'Payload'],
                               [f'Encountered exception in reading contents of files : {ex} ',
                                None]))
        return output_json


if __name__ == '__main__':
    """Call tsv function to export result of list of records to tsv sheet."""
    export_to_tsv('stock_records', 'TSVFile.tsv')
