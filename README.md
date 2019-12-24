# python_import_data
Imports data from a CSV file into an AWS DynamoDB table. The input file is of the format that is exported by DynamoDB when using the "Actions > Export to .csv" function from the console.

## Requirements
Python 3.7.3 with the libraries listed in requirements.txt.<p>

Recommended to create a Python virtual environment with:
<code>
virtualenv `which python3` venv<br>
source venv/bin/activate<br>
pip3 install -r requirements.txt
</code>
