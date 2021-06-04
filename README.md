# NDA Tools

In order to submit data to the National Institute of Mental Health Data Archives (NDA), users must
validate their data to ensure it complies with the required format. This is done using the NDA
validation tool. Additionally, users can package and download data from NDA as well. If associated data is downloaded 
from S3, temporary federated AWS tokens are required. A python package and command line clients have been developed to 
allow users to programmatically validate, package, submit, and/or download data. The package and clients interact with 
the  [Validation](https://nda.nih.gov/api/validation/docs/swagger-ui.html), 
[Submission Package](https://nda.nih.gov/api/submission-package/docs/swagger-ui.html#!), and
[Data Submission](http://nda.nih.gov/api/submission/docs/swagger-ui.html#!) web services.


## Getting Started

### Installing Python and Pip

#### Installing Python
You will need a python distribution to use the client. Run the following from a terminal/command-prompt to determine if python is installed:
```
python --version
```
If python is installed you should see output indicating the version of Python that is installed.

If python is not available, you will need to download and install it from [Python.org](https://www.python.org/). Please also consult the documentation: [Python2](https://docs.python.org/2/using/), [Python3](https://docs.python.org/3/using/)

**Notes:** 
- You may need administrative rights, root, or sudo privileges to install a python distribution.
- Python may be installed but not available on the system path, please consult python installation and usage documentation

#### Installing pip
Since Python 2.7.9 pip is released with python, you can check the version like this:
```
pip --version
```
If pip is installed you should see version information, if not you should install pip. First download it from [https://bootstrap.pypa.io/get-pip.py](https://bootstrap.pypa.io/get-pip.py), then run the following to install for your user.
```
python get-pip.py --user
```

**Notes:** 
- Pip may be installed but not available on the system path, please consult python installation and usage documentation

### Installing the client

These instructions will help you get setup to run the client. 

Simply enter the following command into your terminal or command prompt to install nda-tools:

```pip install nda-tools```

Thi will automatically install the nda-tools package, including the command line scripts and required packages. 

### Credentials
While not needed for just validation, if you would like to create a package and submit your data to the NDA, you must 
have an active account with us. This can be requested from the [NDA website](https://nda.nih.gov/user/create_account.html).
You can read more about what is needed for contributing data into NDA [here](https://nda.nih.gov/contribute.html). 

**You are now ready to run the client.**

Please note that if you encounter SSL errors when running the client, you may need to re-run pip installation of requests, with
`pip install requests[secure]` which will install some additional packages with more support for SSL connections.

## Creating Submissions - vtcmd
To view options available for the validation tool python client, enter the following command:

`vtcmd -h`


### Configuring the Client
- If your command-line inputs have special characters (i.e., passwords) or spaces (i.e, in directory/filenames), you may need to enclose them in quotations.
  - If you are using windows, use double-quotes: ""
  - If you are using Mac OSX or Linux, use single-quotes: ''
- Upon your first run, the client will prompt you to enter your username and password, which it will store in the settings file
at ~/.NDATools/settings.cfg. You may go back and edit your credentials at any time.

The settings.cfg file provided with the client contains configurable options for Endpoints, Files, and User information.

Typically there will be no need to change entries in the 'Endpoints' section, however you may wish to modify the 'Files' and 'User' section with preferred locations for validation results, user login, and AWS credentials information.

- While arguments are not positional, you should make your list of files to validate the first argument.
  - The list of files has no command-line switch so it can get interpreted as part of a preceding argument.
  - For example there is no way to differentiate whether the csv file is part of the -l argument or a second argument: 
  ```
  vtcmd -l "Users/[youruser]/Documents/MultipleDataTypes" \
  "Users/[youruser]/Documents/MultipleDataTypes/Stage_Testing_BigFiles_genomics_sample03.csv" 
  ```  


### Files for Validation
It is important that you know the full path to the csv files that you will be validating. Furthermore, if your data
also includes manifests and/or associated files (ie. genomics files, imaging files), you must also know the full path to these files,
which should be entered as an optional command-line argument. Otherwise, the client will prompt you to enter a list of 
directories where any of the associated files are located. You can also list a bucket, optional prefix, and your AWS 
credentials if your associated files are located in AWS.

**Please note:** When listing the directory for associated files, include the folder **up to but not including**
the file name listed in the csv file.

##### Example:
If the associated file name is Users/[youruser]/Documents/MultipleDataTypes/data/1G_file.fastq and is listed in your csv file as:

>data/1G_file.fastq

then the directory you will enter is:
 >Users/[youruser]/Documents/MultipleDataTypes 

You should **not** include the 'data/' folder as part of the directory name.

To start validation, you must enter a list of files (or file path if not in current directory), separated by a space:

```
vtcmd MultipleDataTypes/genomics_sample03.csv testdata/with_associated_files/genomics_sample03.csv 
```
If your data includes manifest files, you must enter the directories where the manifest files are found, separated by a space:

```
vtcmd submission_data/sample_imagingcollection01.csv  -m submission_data/Manifests
```


If there are associated files, enter the directories where they are found, separated by a space:

```
vtcmd MultipleDataTypes/genomics_sample03.csv testdata/with_associated_files/genomics_sample03.csv -l MultipleDataTypes testdata/with_associated_files 
```

If the files are located somewhere other than the current working directory, then you must enter the full
path to the files:

```
vtcmd MultipleDataTypes/genomics_sample03.csv testdata/with_associated_files/genomics_sample03.csv -l Users/[youruser]/Downloads/SubmissionData testdata/with_associated_files
```

If your associated files are located in S3, then you must include the bucket name, access key, and secret key. 
The access and secret key can be stored in the settings.cfg file as well.

```
vtcmd MultipleDataTypes/genomics_sample03.csv testdata/with_associated_files/genomics_sample03.csv -s3 my_bucket -ak XXXXXXXXXXXXXX -sk XXXXXXXXXXXXXX
```

Note: You can also upload associated files saved locally and in s3. Just make sure to include the directory where the 
local files are saved (-l path/to/local/associated/files)

To create a package, enter "-b" at the end of your command line argument. You can also enter your username, password, 
AWS credentials, collection ID or alternate endpoint title, and the title and description of your submission, or you can enter this
information later when prompted by the client. Until all your files are validated and all associated files have been
located on your local drive or in S3, the client will not begin building the submission package.

When package submission and upload is complete, you will receive an email in your inbox from NDA confirming your 
submission was successful. 

If your file upload was disrupted, you may restart your submission from where if left off if you have the submission ID,
which is provided to you when the submission is first created. To restart the submission using the client, enter the 
following command:

```
vtcmd <submissionID> -r
```

You can optionally include your username, password, AWS credentials, and directory list at this point as well, or enter it if/when prompted
by the client.

## Downloading Data - downloadcmd
To view options available for the download python client, enter:

`downloadcmd -h`

To download data, you should use the downloadcmd command. This provides several options to download your NDA packaged data 
or a subset of the data. All files are downloaded automatically to the ~/AWS_downloads folder, but you can change this by
indicating a new directory in the command line to save files. 

Please note, the maximum transfer limit of data is 5TB at one time. 

### All Package Data
All packaged data can be downloaded by passing the package ID:

`downloadcmd -dp <packageID>`

Note: it will NOT download associated files *unless you created your NDA package with associated files*. Steps to download associated 
files are below.

### Downloading .txt Files
The downloadcmd command has two options for downloading data inside .txt files. If you have downloaded your NDA package, you will find
meta-data .txt files, many of which represent data measures. Genomics, imaging, and other associated data will be listed in these .txt files
as s3 links. If you would like to download all the s3 links in your .txt file, you can indicate so by passing the -ds flag.

`downloadcmd -dp <packageID> -ds path/to/data/structure/file/image03.txt`

Another option is to create your own .txt file listing any and all S3 links you would like to download from NDA. This can
be a subset of the data you want, or a list of everything.

`downloadcmd -dp <packageID> -t path/to/all/s3/txt/file/alls3.txt`

### Restart Download

Often times, your download may be interrupted. To restart a download process, enter the same command you did for the original
run, but include the -r flag and the directory where all the files were being downloaded: 

`downloadcmd -dp <packageID> -t path/to/all/s3/txt/file/alls3.txt -r /Users/<your_user>/AWS_downloads`
 

## Staging Data for Submission – mindar command
The 'mindar' command is a new script added to the nda-tools module to facilitate:
1. creating and deleting miNDARs
2. adding and removing tables from/to miNDARs
3. importing and exporting data from/to miNDARs
4. creating NDA submissions from data stored in a miNDARs   

The mindar command is being provided to select NDA users for the purpose of creating RDB staging environments (miNDAR's) for NDA submission data. The mindar 
command will also automate the process of creating NDA submissions from the miNDAR, which should accelerate the submission process for certain research projects.

**Note** - At this time, this feature is experimental and is only available to a limited selection of users. If you would like access
to this feature, please reach out to the NDA Help Desk for assistance.
### Using the Tool 
   To use the tool, you will need to start by downloading the tool from TestPyPi. After that you can being creating your mindar and adding the necessary data-structures. 
#### Downloading/Installing the tool
The version of NDA tools containing the mindar command is experimental and is currently only available on TestPyPi. 
To install the tool, you will need to run  
```
python -m pip install --index-url https://test.pypi.org/simple/ nda-tools==0.3.0.dev12
```
From time to time, we will be uploading newer versions of this script to TestPyPi. 

Confirm that the tool is installed by running
 ```
 mindar -h
```   
## Creating an empty miNDAR
Enter the following command to create an empty miNDAR
```
mindar create --nickname  <name here> --mpasword <mindar-password-here>
```
After this command completes, an empty miNDAR for your user will have been created. The connection information required to connect directly to this DB will be output to the command line. 
 
**Note** - In Oracle terminology, schema is equivalent to a DB user. In this document, we use the terms schema and username interchangeably. 
 
##### Command line args  
- **mpassword** - _required_ - the password entered here will be the password that you need to use in order to connect to the DB
- **nickname** - _optional_ - the value provided here will become the name of the mindar and associated package. 
If none is provided, a default name of 'MINDAR_BLANK_PKG_XXXXX' will be assigned, where XXXXX is the package-id created for the mindar.

## Showing existing miNDAR
The following command will show all miNDARs associated with the current user. 
```
  mindar show --include-deleted
``` 
After this command completes, a table will be output to the console containing basic information about each miNDAR associated with the current user.
 
##### Command line args
- **include-deleted** - _optional_ - if specified, the output will include miNDARs that have been deleted.
     
## Deleting a miNDAR
This command is provided in the event that a miNDAR is no longer required or if a miNDAR becomes corrupt and the user would prefer to start over from scratch. 

The following command will delete a miNDAR 
```
mindar delete --force <schema> 
```
After this command completes, a Delete request will be initiated for the miNDAR specified. The user will be prompted for confirmation unless the --force flag is specified.

##### Command line args 
- **force** - _optional_ - If provided, no confirmation message will be printed to the console. 
- **schema** - _required_ - The schema corresponding to the mindar to be deleted.
     
## Adding tables to a miNDAR
Enter the following command to add a table to a miNDAR
```
mindar tables add image03,genomics03,datastructureexample07  --schema <schema>
``` 
This command will attempt to add each table specified at the command line to the miNDAR, one by one. If an error is encountered during the addition of one of the tables to the miNDAR, the tool will display an error message and continue onto the next table in the list. The tables argument must be a comma delimited list without spaces. If a table is specified on the command line that already exists in the miNDAR, it will be skipped over during processing and the data in that table will not be affected by the command.

##### Command line args
- **tables** - _required_ - comma delimited list of data-structure short-names to add to miNDAR. The list of data-structures that are available to add come from the shared data-structure list available via the data-dictionary api. (https://nda.nih.gov/api/datadictionary/docs/swagger-ui.html) 
- **schema** - _required_ - schema corresponding to the miNDAR to add tables to.
     
## Showing existing tables in a miNDAR
The following command will show all the tables contained in a particular miNDAR. 
```
mindar describe --refresh-stats <schema>
``` 
After this command completes, a table will be output to the console containing basic information about each table inside the miNDAR with the specified schema
 
##### Command line args
- **refresh-stats** - _optional_ - if specified, stats will be gathered for the schema before retrieving table information. Gathering stats will make the number of rows displayed for each table more accurate. The approximate number of rows is retrieved from the all_tab_columns table, which reflects the most recent stats gathered by the DB. For more information about gathering schema statistics, see https://docs.oracle.com/cd/A84870_01/doc/server.816/a76992/stats.htm
     
## Dropping tables from a miNDAR
Enter the following command to remove a table to a miNDAR
```
mindar tables drop image03,genomics03,datastructureexample07  --schema <schema>
``` 
This command will attempt to drop each table specified at the command line to the miNDAR, one by one. If an error is encountered during the processing of one of the tables, the tool will display an error message and continue onto the next table in the list. The tables argument must be a comma delimited list without spaces. If a table is specified on the command line that does not exist in the miNDAR, it will be skipped over during processing and the data in that table will not be affected by the command.
    
## Recreating tables in a miNDAR
This command is provided in the even that a user wants to undo edits made to table structures. 

Enter the following command to drop and re-add tables to a miNDAR. 
```
  mindar tables reset image03,genomics03,datastructureexample07  --schema <schema>
``` 
This command will attempt to drop each table specified (if it exists in the miNDAR) and then re-add the table to the miNDAR. At the end of this command, each table will not contain any rows and the table structure will match the latest structure definition retrieved from the data-dictionary API (https://nda.nih.gov/api/datadictionary/docs/swagger-ui.html). If an error is encountered during the processing of one of the tables, the tool will display an error message and continue onto the next table in the list. The tables argument must be a comma delimited list without spaces. 

## Further Assistance
If you have any problems with this validation tool python client, or would like to provide feedback/comments,
please email us at NDAHelp@mail.nih.gov.