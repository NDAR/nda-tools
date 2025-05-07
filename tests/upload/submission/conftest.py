import pytest


@pytest.fixture
def package_json():
    return {
        "_links": {
            "self": {
                "href": "http://nda.nih.gov/api/submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9"
            }
        },
        "validation_results": [
            {
                "short_name": "ndar_subject01",
                "_links": {
                    "self": {
                        "href": "http://nda.nih.gov/api/submission-package/e33cceb2-fb6a-4444-bb04-782ab7495a46"
                    }
                },
                "id": "e33cceb2-fb6a-4444-bb04-782ab7495a46",
                "scope": None
            }
        ],
        "package_info": {
            "collection_id": 1860,
            "dataset_name": "sdfgasdf",
            "dataset_description": "asdfasdfasfd",
            "endpoint_title": "test",
            "status": "complete",
            "replacement_submission": None
        },
        "submission_package_uuid": "4fafc302-51fc-4a5c-bc92-cd41548c98b9",
        "created_date": "2025-05-07T12:33:12.092-04:00",
        "expiration_date": "2025-05-08T12:33:12.092-04:00",
        "files": [
            {
                "id": "9890147",
                "type": "Submission Data File",
                "path": "s3://nimhda-submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9/NDASubmission_gregmagdits_1746635592093/C_submission.xml",
                "package_resource_id": "4fafc302-51fc-4a5c-bc92-cd41548c98b9",
                "_links": {
                    "download": {
                        "href": "http://nda.nih.gov/api/submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9/file/9890147/download"
                    }
                }
            },
            {
                "id": "9890150",
                "type": "Submission Data Package",
                "path": "s3://nimhda-submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9/NDASubmission_gregmagdits_1746635592093/NDARSubmissionPackage-1746635592093.zip",
                "package_resource_id": "4fafc302-51fc-4a5c-bc92-cd41548c98b9",
                "_links": {
                    "download": {
                        "href": "http://nda.nih.gov/api/submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9/file/9890150/download"
                    }
                }
            },
            {
                "id": "9890148",
                "type": "Submission Ticket",
                "path": "s3://nimhda-submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9/NDASubmission_gregmagdits_1746635592093/NDARSubmissionPackage-1746635592093.xml",
                "package_resource_id": "4fafc302-51fc-4a5c-bc92-cd41548c98b9",
                "_links": {
                    "download": {
                        "href": "http://nda.nih.gov/api/submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9/file/9890148/download"
                    }
                }
            },
            {
                "id": "9890149",
                "type": "Submission Memento",
                "path": "s3://nimhda-submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9/NDASubmission_gregmagdits_1746635592093/.submissionInfo",
                "package_resource_id": "4fafc302-51fc-4a5c-bc92-cd41548c98b9",
                "_links": {
                    "download": {
                        "href": "http://nda.nih.gov/api/submission-package/4fafc302-51fc-4a5c-bc92-cd41548c98b9/file/9890149/download"
                    }
                }
            }
        ],
        "status": "complete",
        "errors": ""
    }
