
# CHANGELOG

## 0.2.0 - 2020-04-07
* [`vtcmd`] Fixed bug that prevented resuming submissions when incomplete multipart uploads were present in s3 buckets.
* [`vtcmd`] Updated code to print current version of tool to console before running.

## 0.1.21 - 2019-10-30
* [`vtcmd`] Added --skipValidation flag to skip checking that local associated files exist on filesystem.
* [`vtcmd`] New sections and options are automatically added to user settings.cfg.
* [`nda-tools`] Fixed bugs preventing use of full file paths on all operating systems.
* [`nda-tools`] Replaced use of aws_access_key and aws_secret_key with iam_user_credentials and data_manager_credentials, which defaults to use of FederationUser Token to submit from S3 objects if no credentials are supplied in settings.cfg.
* [`nda-tools`] Added unit tests for Utils.py for parse relative/full filepaths and automatically sanitizing paths based on path patterns.

## 0.1.20 - 2019-07-15
* [`nda-tools`] Python2 compatibility fixes and some PEP-8 style changes; fixes [GitHub Issue 9](https://github.com/NDAR/nda-tools/issues/9)
* [`nda-tools`] Addressed issue where configuration file cannot be located on fresh installations.

## 0.1.19 - 2019-07-02
* [`vtcmd`] Fixed issue with writing debug log files to directory before it is created; fixes [GitHub Issue 12](https://github.com/NDAR/nda-tools/issues/12)
* [`vtcmd`] Removed write mode when opening associated files for upload to S3
* [`vtcmd`] Update to only fetch multi-part credentials once during resume option in check_submitted_files()
* [`downloadcmd`] Add message regarding transfer limit to download(5TB)
* [`downloadcmd`] Update to use alias when creating local file path to download
* [`nda-tools`] Update to check version of the client and service
* [`nda-tools`] Add CHANGELOG.md

## 0.1.18 - 2019-05-15
* [`vtcmd`] Update to catch expired token errors and get new credentials so long-running file uploads can complete successfully.
* [`vtcmd`] Update status to "In Progress" in Python client so that this indicates that uploading of files was initiated until it's completed
* [`nda-tools`] Add debug logging to service calls for the responses/exceptions caught when communicating with services so that improves debugging and issues resolution.

## 0.1.17 - 2019-04-05
* [`vtcmd`] Decreased batch size for fetching multi-part credentials and updating submission status

## 0.1.16 - 2019-03-27
* [`vtcmd`] Update client to check that associated files are readable during associate file step before creating submission package
* [`vtcmd`] Add support for resuming partially uploaded files when resuming a submission so that it will check which parts are already uploaded, and then only finish uploading the incomplete parts.
* [`vtcmd`] Update to set the size for both datafile and manifest associated files before uploading files so that submission metrics accurately reflect the state of a submission
* [`nda-tools`] Updated make_config function so that user can use both versions python 2 and python 3

## 0.1.15 -  2018-12-20
* [`vtcmd`] Update to handle new message format from services that includes error message details 
* [`vtcmd`] Update to use batch operations for getting multipart upload credentials and for updating status of submission files so that submission of large numbers of files will take less time.

