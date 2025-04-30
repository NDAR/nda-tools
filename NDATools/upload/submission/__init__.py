import logging

logger = logging.Logger(__name__)


class NdaSubmission:
    def __init__(self, collection_id, title, description, validated_files):
        self.collection_id = collection_id
        self.title = title
        self.description = description
        self.validated_files = validated_files


class NdaReSubmission(NdaSubmission):
    def __init__(self, submission_id, collection_id, title, description, validated_files):
        super().__init__(collection_id, title, description, validated_files)
        self.submission_id = submission_id
