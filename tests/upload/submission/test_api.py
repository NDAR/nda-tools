import time
from unittest.mock import MagicMock

import pytest

import NDATools
from NDATools.upload.submission.api import SubmissionApi, CollectionApi, SubmissionPackageApi, PackagingStatus


@pytest.fixture
def submission_api():
    return SubmissionApi('https://nda.nih.gov/api/submission', 'testusername',
                         'testpassword')


@pytest.fixture
def submission_json():
    return {
        "submission_status": "Upload Completed",
        "dataset_title": "ABCD",
        "dataset_description": "ABCD",
        "dataset_created_date": "2025-01-17T09:58:55.963-05:00",
        "dataset_modified_date": None,
        "_links": {"files": {"href": "https://nda.nih.gov/api/submission/12345/files"}},
        "submission_id": "12345",
        "collection": {
            "id": "1860",
            "title": "Test"
        }
    }


@pytest.fixture
def change_history_json():
    return [{"created_date": "2025-01-17T09:58:55.963-05:00", "created_by": "testuser",
             "submission_package": "f90d5181-a916-4da4-8483-c1fa348214bb", "manual_approval_required": False,
             "approval_status": "", "replacement_authorized": False,
             "resourceId": "f90d5181-a916-4da4-8483-c1fa348214bb", "links": []}]


@pytest.fixture
def submission_details_json():
    return {
        "submissionId": 66211,
        "validation_uuids": [
            "9087786b-b85a-469f-98e5-dc3dcfe93041"
        ],
        "pendingChanges": [
            {
                "pending_change_id": 65827,
                "dataStructureId": 13650,
                "shortName": "fmriresults01",
                "rows": 40,
                "validationUuids": [
                    "9087786b-b85a-469f-98e5-dc3dcfe93041"
                ],
                "flaggedRecordCount": 0,
                "hasQaErrors": False
            }
        ]
    }


def test_submission_api_get_submission(submission_api, monkeypatch, submission_json):
    with monkeypatch.context() as m:
        m.setattr(NDATools.upload.submission.api, "get_request", MagicMock(return_value=submission_json))
        s = submission_api.get_submission(int(submission_json['submission_id']))
        assert s.submission_id == int(submission_json['submission_id'])


def test_submission_api_get_submission_version(submission_api, monkeypatch, change_history_json):
    with monkeypatch.context() as m:
        m.setattr(NDATools.upload.submission.api, "get_request", MagicMock(return_value=change_history_json))
        sh = submission_api.get_submission_history(12345)
        assert len(sh) == len(change_history_json)


def test_submission_details(submission_api, monkeypatch, submission_details_json):
    with monkeypatch.context() as m:
        m.setattr(NDATools.upload.submission.api, "get_request", MagicMock(return_value=submission_details_json))
        sd = submission_api.get_submission_details(12345)
        assert sd.submission_id == submission_details_json['submissionId']
        assert sd.get_data_structure_details('ndar_subject01') is None
        assert sd.get_data_structure_details('fmriresults01') is not None
        assert sd.get_data_structure_details('fmriresults01').short_name == 'fmriresults01'


@pytest.fixture
def collection_api():
    return CollectionApi('https://nda.nih.gov/api/validationtool/v2', 'testusername', 'testpassword')


@pytest.fixture
def collections_json():
    return [
        {
            "id": 1860,
            "title": "Test"
        },
        {
            "id": 1861,
            "title": "Test2"
        }
    ]


def test_collection_api_get_collections(collection_api, collections_json, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(NDATools.upload.submission.api, "get_request", MagicMock(return_value=collections_json))
        collections = collection_api.get_user_collections()
        assert len(collections) == len(collections_json)


@pytest.fixture
def submission_package_api():
    return SubmissionPackageApi('https://nda.nih.gov/api/submission-package', 'testusername', 'testpassword')


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


def test_sub_package_api_build_package(submission_package_api, package_json, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(NDATools.upload.submission.api, "post_request", MagicMock(return_value=package_json))

        package = submission_package_api.build_package(1860, 'sdfgasdf', 'asdfasdfasfd',
                                                       ["e33cceb2-fb6a-4444-bb04-782ab7495a46"])
        assert package.submission_package_uuid == package_json['submission_package_uuid']
        assert package.status == package_json['package_info']['status']
        assert package.created_date == package_json['created_date']
        assert package.expiration_date == package_json['expiration_date']


def test_sub_package_api_wait_package_complete(submission_package_api, package_json, monkeypatch):
    incomplete_package = {**package_json, 'status': PackagingStatus.PROCESSING}
    with monkeypatch.context() as m:
        m.setattr(NDATools.upload.submission.api, "get_request",
                  MagicMock(side_effect=[incomplete_package, package_json]))
        m.setattr(time, "sleep", MagicMock(return_value=None))
        package = submission_package_api.wait_package_complete(package_json['submission_package_uuid'])
        assert package.submission_package_uuid == package_json['submission_package_uuid']
        assert package.status == package_json['package_info']['status']
        assert package.created_date == package_json['created_date']
        assert package.expiration_date == package_json['expiration_date']
