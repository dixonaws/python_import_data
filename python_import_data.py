import boto3
import sys
import csv
import argparse


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

        print("lst_metadata: " + str(lst_metadata))

        for row in csv_reader:  # row is a list of strings representing a record in the csv file
            dict_item = {}

            for col in range(0, len(lst_metadata)):
                dict_item[lst_metadata[col]] = row[col]

            lst_items.append(dict_item)
        # for row in csv_reader

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

# import data from a CSV file into a dynamodb table
# tabledef is in the following format:

# "action_name (S)","prompt (S)","steps (M)"
# "cameraAny","your vehicle does not have camera module installed, displaying optional modules on central display, prices start at USD$499 excluding tax.","{  ""DisplayPanel"" : { ""M"" : {      ""end"" : { ""N"" : ""9"" },      ""start"" : { ""N"" : ""0"" }    }  }}"
# "climateControlFanSpeedDecrease","press down Left Fan Speed Button or Right Fan Speed Button","{  ""LeftFanSpeedButton"" : { ""M"" : {      ""end"" : { ""N"" : ""4"" },      ""start"" : { ""N"" : ""0"" }    }  },  ""RightFanSpeedButton"" : { ""M"" : {      ""end"" : { ""N"" : ""4"" },      ""start"" : { ""N"" : ""0"" }    }  }}"
# "climateControlFanSpeedIncrease","lift up Left Fan Speed Button or Right Fan Speed Button","{  ""LeftFanSpeedButton"" : { ""M"" : {      ""end"" : { ""N"" : ""4"" },      ""start"" : { ""N"" : ""0"" }    }  },  ""RightFanSpeedButton"" : { ""M"" : {      ""end"" : { ""N"" : ""4"" },      ""start"" : { ""N"" : ""0"" }    }  }}"
# "climateControlRearSeatsChange","press down zone control button to select rear seats option and use temperature dial on left or right to adjust the temperature.","{  ""LeftTemperatureDial"" : { ""M"" : {      ""end"" : { ""N"" : ""5"" },      ""start"" : { ""N"" : ""4"" }    }  },  ""RightTemperatureDial"" : { ""M"" : {      ""end"" : { ""N"" : ""7.5"" },      ""start"" : { ""N"" : ""5"" }    }  },  ""ZoneControlButton"" : { ""M"" : {      ""end"" : { ""N"" : ""4"" },      ""start"" : { ""N"" : ""0"" }    }  }}"
# "climateControlSynchronize","lift up zone control button to select sync option. Alternatively, press and hold left temperature dial.","{  ""LeftTemperatureDial"" : { ""M"" : {      ""end"" : { ""N"" : ""7.5"" },      ""start"" : { ""N"" : ""5.5"" }    }  },  ""ZoneControlButton"" : { ""M"" : {      ""end"" : { ""N"" : ""5.5"" },      ""start"" : { ""N"" : ""0"" }    }  }}"
