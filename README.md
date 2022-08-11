# nda-tools

To submit data to the National Institute of Mental Health Data Archives (NDA), users must
validate their data to ensure it complies with the required format. This is done using the NDA
Validation and Upload tool. Additionally, users can package and download data from NDA as well. If associated data is downloaded 
from S3, temporary federated AWS tokens are required. A Python package and command line clients have been developed to 
allow users to programmatically validate, package, submit, and/or download data. The package and clients interact with 
the [Validation](https://nda.nih.gov/api/validation/swagger-ui.html), 
[Submission Package](https://nda.nih.gov/api/submission-package/swagger-ui.html#!), and
[Data Submission](http://nda.nih.gov/api/submission/swagger-ui.html#!) web services.


## Getting Started

### Installing Python
You need a Python distribution to use the client. Run the following from a terminal/command-prompt to determine if Python 
is installed:
```
python3 --version
```
If Python is installed you should see output indicating the version of Python that is installed.

If Python is not available, you will need to download and install it from [Python.org](https://www.python.org/). 
Please also consult the documentation: [Python3](https://docs.python.org/3/using/)

**Notes:** 
- You may need administrative rights, root, or sudo privileges to install a Python distribution.
- Python may be installed but not available on the system path, please consult Python installation and usage documentation.

### Installing pip
Since Python 2.7.9 pip is released with Python, you can check the version with:
```
pip --version
```
If pip is installed, you should see version information, if not you should install pip. First download it from 
[https://bootstrap.pypa.io/get-pip.py](https://bootstrap.pypa.io/get-pip.py), then run the following to install for your user.
```
python3 get-pip.py --user
```

**Notes:** 
- Pip may be installed but not available on the system path, please consult Python installation and usage documentation.

### Installing the client

These instructions will help you get setup to run the client. 

Simply enter the following command into your terminal or command prompt to install nda-tools:

```pip install nda-tools```

This will automatically install the nda-tools package, including the command line scripts and required packages. 


### Credentials
While not needed solely for validation, if you want to create a package and submit your data to the NDA, you must 
have an active account with us. This can be requested from the [NDA website](https://nda.nih.gov/user/dashboard/profile.html).
You can read more about what is needed for contributing data into NDA [here](https://nda.nih.gov/contribute.html). 

_Starting from version 0.2.18 of nda-tools, password storage in the settings.cfg and the --password / -p flags have been 
replaced with keyring._

#### Keyring

Keyring is Python package that leverages the operating system's credential manager to securely store and retrieve
user credentials. To improve security on nda-tools, password storage in the settings.cfg and the password flag
have been replaced with keyring. Furthermore, the keyring implementation supports long-running workflows.

##### Updating Stored Passwords with keyring
###### All Operating Systems
For users of any operating system, the password can be updated with:

`keyring.set_password('nda-tools', USERNAME, NEW_PASSWORD)`
######Mac / Windows
Mac and Windows users may use Keychain and Credentials Manager, respectively, to update their passwords.

To update your password with keyring, run `keyring.set_password('nda-tools', 'YOUR_USERNAME', 'NEW_PASSWORD')`, 
replacing _YOUR_USERNAME_ and _NEW_PASSWORD_ with your NDA username and new password. You can read more from
the [Keyring Documentation](https://pypi.org/project/keyring/).

If you do not have any entries stored via keyring, you will be prompted to enter the password. If authentication is successful, 
nda-tools will store your password via keyring. Subsequent usage of nda-tools will retrieve the password automatically
and securely from keyring.

###### Linux
Linux users may need to install a backend implementation of keyring since they may not have a native credentials manager 
such as those included with the Mac and Windows operating systems. If the keyring backend is missing, nda-tools will print the message:

`If there is no backend set up for keyring, you may try pip install secretstorage --upgrade keyrings.alt`

For Ubuntu users,

`apt-get install -y gnome-keyring`

##### You are Now Ready to Run the Client.

Please note that if you encounter SSL errors when running the client, you may need to re-run pip installation of requests, with
`pip install requests[secure]` which will install some additional packages with more support for SSL connections.

## Using the Client
To view options available for the Validation Tool Python client, enter the following command:

`vtcmd -h`

or to view options available for the Download Python client, enter:

`downloadcmd -h`

### Configuring the Client
- If your command-line inputs have special characters (i.e., passwords) or spaces (i.e., in directory/filenames), 
  you may need to enclose them in quotations.
  - If you are using windows, use double-quotes: ""
  - If you are using Mac OSX or Linux, use single-quotes: ''
- Upon your first run, the client will prompt you to enter your username and password, which it will store in your operating system's credential manager. You may go back and edit your credentials at any time.

The settings.cfg file provided with the client contains configurable options for Endpoints, Files, and User information.

Typically, you won't need to change entries in the 'Endpoints' section, however you might wish to modify the 'Files' and 'User' section with preferred locations for validation results, user login, and AWS credentials information.

- While arguments are not positional, the first argument should be the list of files to validate.
  - The list of files has no command-line switch so it can get interpreted as part of a preceding argument.
  - For example, there is no way to differentiate whether the csv file is part of the -l argument or a second argument: 
  ```
  vtcmd -l "Users/[youruser]/Documents/MultipleDataTypes" \
  "Users/[youruser]/Documents/MultipleDataTypes/Stage_Testing_BigFiles_genomics_sample03.csv" 
  ```  


### Files for Validation
It is important that you know the full path to the csv files that you will be validating. Furthermore, if your data
also includes manifests and/or associated files (i.e., genomics files, imaging files, etc.), you must also know the full 
path to these files, which should be entered as an optional command-line argument. Otherwise, the client will prompt you
to enter a list of directories where any of the associated files are located. You can also list a bucket, optional prefix,
and your AWS credentials if your associated files in AWS.

**Please note:** When listing the directory for associated files, include the folder **up to but not including**
the file name listed in the csv file.

##### Example:
If the associated file name is Users/[youruser]/Documents/MultipleDataTypes/data/1G_file.fastq and is listed in your csv file as:

>data/1G_file.fastq

then the directory you will enter is:
 >Users/[youruser]/Documents/MultipleDataTypes 

You should **not** include the 'data/' folder as part of the directory name.

To start validation, you must enter a list of files (or a file path if not in current directory), separated by a space:

```
vtcmd MultipleDataTypes/genomics_sample03.csv testdata/with_associated_files/genomics_sample03.csv 
```
If your data includes manifest files, you must enter the directories where the manifest files are located, separated by a space:

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

If your associated files are in S3, then you must include the bucket name, access key, and secret key. 
The access and secret key can be stored in the settings.cfg file as well.

```
vtcmd MultipleDataTypes/genomics_sample03.csv testdata/with_associated_files/genomics_sample03.csv -s3 my_bucket -ak XXXXXXXXXXXXXX -sk XXXXXXXXXXXXXX
```

Note: You can also upload associated files saved locally and in s3. Just make sure to include the directory where the 
local files are saved (-l path/to/local/associated/files)

To create a package, enter "-b" at the end of your command line argument. You can also enter your username, 
AWS credentials, Collection ID or alternate endpoint title, and the title and description of your submission, or you can enter this
information later when prompted by the client. The client will not begin building the submission package until:
- All your files are validated
- All associated files have been located on your local drive or in S3

Once package submission and upload are complete, you will receive an email in your inbox from NDA confirming your 
submission was successful. 

If your file upload was disrupted, you may restart your submission from where if left off if you have the submission ID,
which is provided to you when the submission is first created. To restart the submission using the client, enter the 
following command:

```
vtcmd <submissionID> -r
```

Optionally, you can include your username, AWS credentials, and directory list at this point as well, or enter it if/when prompted
by the client.

## Downloading Data

To download data, you should use the downloadcmd command. This provides several options to download your NDA packaged data 
or a subset of the data. All files are downloaded automatically to the ~/NDA/nda-tools/<package-id> folder, but you can change this by
indicating a new directory in the command line to save files. 

Please note, the maximum transfer limit of data is 20TB per month. 

#### All Package Data
All packaged data can be downloaded by passing the package ID:

`downloadcmd -dp <packageID>`

Note: it will NOT download associated files *unless you created your NDA package with associated files*. 
Steps to download associated files are below.

#### Downloading .txt Files
The downloadcmd command has two options for downloading data inside .txt files. If you downloaded your NDA package, you 
will find meta-data .txt files, many of which represent data measures. Genomics, imaging, and other associated data 
will be listed in these .txt files as s3 links. If you would like to download all the s3 links in your .txt file, you 
can indicate so by passing the -ds flag.

`downloadcmd -dp <packageID> -ds path/to/data/structure/file/image03.txt`

Another option is to create your own .txt file listing any and all S3 links you would like to download from NDA. This can
be a subset of the data you want, or a list of everything.

`downloadcmd -dp <packageID> -t path/to/all/s3/txt/file/alls3.txt`


#### Restart Download

Often, your download may be interrupted. To restart a download process, enter the same command you did for the original
run, but include the -r flag and the directory where all the files were being downloaded:

`downloadcmd -dp <packageID> -t path/to/all/s3/txt/file/alls3.txt -r /Users/<your_user>/AWS_downloads`
 


## Further Assistance
If you have any problems with this Validation Tool Python client, or would like to provide feedback/comments,
please email us at NDAHelp@mail.nih.gov.
# nda-tools
