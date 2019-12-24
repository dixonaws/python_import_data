import csv
import json
import sys
import boto3

def main():
    str_filename="awsauto_manual-ddb-table.csv"

    data={}

    with open(str_filename, "r") as file_csv:
        csv_reader=csv.DictReader(file_csv)

        for row in csv_reader:
            id=row['action_name (S)']
            data['action_name (S)']=row

        print(data)

main()