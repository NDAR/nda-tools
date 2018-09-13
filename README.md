#NDAR Validation Tool Python Client
In order to submit data to the National Institute of Mental Health Data Archives (NDA), users must
validate their data to ensure it complies with the required format. This is done using the NDAR
validation tool. A python client for this tool has been developed to allow users to programmatically
validate, package, and submit their data. The client interacts with the [Validation](https://stage.nimhda.org/api/validation/docs/swagger-ui.html),
[Submission Package](https://stage.nimhda.org/api/submission-package/docs/swagger-ui.html#!), and
[Data Submission](http://stage.nimhda.org/api/submission/docs/swagger-ui.html#!) web services.


##Getting Started

### Installing Python
You will need a python distribution to use the client. Run the following from a terminal/command-prompt to determine if python is installed:
```
python --version
```
If python is installed you should see output indicating the version of Python that is installed.

If python is not available, you will need to download and install it from [Python.org](https://www.python.org/). Please also consult the documentation: [Python2](https://docs.python.org/2/using/), [Python3](https://docs.python.org/3/using/)

**Notes:** 
- You may need administrative rights, root, or sudo privileges to install a python distribution.
- Python may be installed but not available on the system path, please consult python installation and usage documentation

### Installing pip
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

These instructions will help you get setup to run the client. You should have a copy of the the files listed below as 
part of your download. 

If you haven't yet, you should either [clone the repository using git](https://help.github.com/articles/cloning-a-repository/) or download the files individually. If you are downloading from GitHub a zip file containing everything is available.

- nda-validationtool-client.py
- README.md (this file)
- requirements.txt
- settings.cfg
- setup.py


run the setup.py installation script to have the validation-client available as an executable from the system path, and to have setuptools install dependencies: boto3, requests, and tqdm

```
python setup.py install --user 
```

###Configuring the Client
The settings.cfg file provided with the client contains configurable options for Endpoints, Files, and User information.

Typically there will be no need to change entries in the 'Endpoints' section, however you may wish to modify the 'Files' and 'User' section with preferred locations for validation results, and user login information.

###Credentials
While not needed for just validation, if you would like to create a package and submit your data to the NDA, you must 
have an active account with us. This can be requested from the [NDAR website](https://ndar.nih.gov/user/create_account.html).
You can read more about what is needed for contributing data into NDAR [here](https://ndar.nih.gov/contribute.html). 

#####You are now ready to run the client.

Please note that if you encounter SSL errors when running the client, you may need to re-run pip installation of requests, with
`pip install requests[secure]` which will install some additional packages with more support for SSL connections.

##Using the Client
To view options available for the validation tool python client, enter the following command:

`python nda-validationtool-client.py -h`

**Notes:**
- While arguments are not positional, you should make your list of files to validate the first argument.
  - The list of files has no command-line switch so it can get interpreted as part of a preceding argument.
  - For example there is no way to differentiate whether the csv file is part of the -l argument or a second argument: 
  ```
  python nda-validationtool-client.py -l "Users/[youruser]/Documents/MultipleDataTypes" \
  "Users/[youruser]/Documents/MultipleDataTypes/Stage_Testing_BigFiles_genomics_sample03.csv" 
  ```  
- If your command-line inputs have special characters (i.e., passwords) or spaces (i.e, in directory/filenames), you will need to enclose them in quotations.
  - If you are using windows, use double-quotes: ""
  - If you are using Mac OSX or Linux, use single-quotes: ''
- You may also enter usernames and passwords in the settings.cfg file

###Files for Validation
It is important that you know the full path to the csv files that you will be validating. Furthermore, if your data
also includes associated files (ie. genomics files, imaging files), you must also know the full path to these files,
which should be entered as an optional command-line argument. Otherwise, the client will prompt you to enter a list of 
directories where any of the associated files are located.

**Please note:** When listing the directory for associated files, include the folder **up to but not including**
the file name listed in the csv file.

#####Example:
If the associated file name that is listed in your csv file is:

>data/1G_file.fastq

then the directory you will enter is:
 >Users/[youruser]/Documents/MultipleDataTypes 

You should **not** include the 'data/' folder as part of the directory name.

To start validation, you must enter a list of files (or file path if not in current directory), separated by a space:

```
python nda-validationtool-client.py MultipleDataTypes/Stage_Testing_BigFiles_genomics_sample03.csv testdata/with_associated_files/genomics_sample03_good_relative_path.csv 
```

If there are associated files, enter the directories where they are found, separated by a space:

```
python nda-validationtool-client.py MultipleDataTypes/Stage_Testing_BigFiles_genomics_sample03.csv testdata/with_associated_files/genomics_sample03_good_relative_path.csv -l MultipleDataTypes testdata/with_associated_files 
```

If the files are located somewhere other than the current working directory, then you must enter the full
path to the files:

```
python nda-validationtool-client.py MultipleDataTypes/Stage_Testing_BigFiles_genomics_sample03.csv testdata/with_associated_files/genomics_sample03_good_relative_path.csv -l Users/[youruser]/Downloads/SubmissionData testdata/with_associated_files
```

To create a package, enter "-b" at the end of your command line argument. You can also enter your username, password,
collection ID or alternate endpoint title, and the title and description of your submission, or you can enter this
information later when prompted by the client. Until all your files are validated and all associated files have been
located on your local drive, the client will not begin building the submission package.

When package submission and upload is complete, you will receive an email in your inbox from NDA confirming your 
submission was successful. 

If your file upload was disrupted, you may restart your submission from where if left off if you have the submission ID,
which is provided to you when the submission is first created. To restart the submission using the client, enter the 
following command:

```
python nda-validationtool-client.py <submissionID> -r
```

You can optionaly include your username, password, and directory list at this point as well, or enter it when prompted
by the client.


##Further Assistance
If you have any problems with this validation tool python client, or would like to provide feedback/comments,
please email us at NDAHelp@mail.nih.gov.
