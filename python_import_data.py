import boto3
import sys
import csv
import argparse
import json

def main():
    parser = argparse.ArgumentParser(
        description="Specify the number of records to generate and the filename to write, e.g. generate_records.py 1000 my_csv_file.csv")

    parser.add_argument("csvfile", help="The path to the input file.")
    parser.add_argument("table_name",
                        help="The name of the target DynamoDB table.")

    args = parser.parse_args()

    str_filename = str(args.csvfile)

    str_table_name = str(args.table_name)

    # lst_metadata will contain the values from the header row
    lst_metadata = []

    # lst_items will contain a dict representing an item/record from the CSV file
    lst_items = []

    # lst_header_row will contain the first row of the CSV file
    lst_header_row = []

    lst_data_types=[]

    dynamo_resource = boto3.resource("dynamodb", region_name="us-east-1")

    sys.stdout.write("Opening connection to dynamodb service... ")
    tbl_table = dynamo_resource.Table(str_table_name)
    print("done.")

    sys.stdout.write("Opening CSV file (" + str_filename + ")... ")

    with open(str_filename, "r", newline='') as file_csv:
        csv_reader = csv.reader(file_csv, delimiter=',', quotechar='"')

        # read the first line of the file (the header row)
        lst_header_row = csv_reader.__next__()
        print("done, input file has " + str(len(lst_header_row)) + " columns.")

        for field in lst_header_row:
            str_column_name = field.split(" ")[0]
            str_column_type = field.split(" ")[1]
            lst_metadata.append(str_column_name)
            lst_data_types.append(str_column_type)

        print("lst_metadata: " + str(lst_metadata))
        print("lst_data_types: " + str(lst_data_types))

        for row in csv_reader:  # row is a list of strings representing a record in the csv file
            dict_item = {}

            for col in range(0, len(lst_metadata)):
                # perform type conversion
                if(lst_data_types[col]=="(N)"):
                    # print(lst_metadata[col] + " is a number type, performing type conversion...")
                    data=row[col]
                    converted_data=int(data)
                    dict_item[lst_metadata[col]] = converted_data
                elif(lst_data_types[col]=="(BOOL)"):
                    # print(lst_metadata[col] + " is a bool type, performing type conversion...")
                    data=row[col]
                    if(data=="true"): converted_data=True
                    if(data=="false"): converted_data=False
                    dict_item[lst_metadata[col]] = converted_data
                else:
                    dict_item[lst_metadata[col]] = row[col]

            lst_items.append(dict_item)
        # for row in csv_reader

    # debugging - print the list of items to import before doing the work
    # print(json.dumps(lst_items, indent=4))

    # put each item into the dynamo table
    sys.stdout.write("Inserting items into " + str_table_name + "... ")
    for item in lst_items:
        response = tbl_table.put_item(
            TableName=str_table_name,
            Item=item
        )  # put_item()

        # print(response)

    print("done.")

main()
