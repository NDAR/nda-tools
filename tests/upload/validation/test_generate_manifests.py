import json
from pathlib import Path

import pytest

from NDATools.upload.validation.manifests import ManifestRecord, generate_manifests


class TestManifestRecord:
    def test_manifest_record_init(self):
        record = ManifestRecord(path="dir/file.txt", name="file.txt", md5sum="abc", size=100)
        assert record.path == "dir/file.txt"
        assert record.name == "file.txt"
        assert record.md5sum == "abc"
        assert record.size == 100

    def test_manifest_record_init_optional(self):
        record = ManifestRecord(path="dir/file.txt", name="file.txt")
        assert record.path == "dir/file.txt"
        assert record.name == "file.txt"
        assert record.md5sum is None
        assert record.size is None


class TestGenerateManifests:
    @pytest.fixture
    def test_dir(self, tmp_path):
        tmpdir = tmp_path / "test_generate_manifests"
        tmpdir.mkdir()

        subject_dir = tmpdir / "subject"
        subject_dir.mkdir()

        # Create subdirectories and files
        dir1 = subject_dir / "dir1"
        dir1.mkdir()
        (dir1 / "file1.txt").write_text("content1")
        (dir1 / "file2.csv").write_text("content2")

        subdir = dir1 / "subdir"
        subdir.mkdir()
        (subdir / "file3.txt").write_text("content3")

        dir2 = subject_dir / "dir2"
        dir2.mkdir()
        (dir2 / "file4.txt").write_text("content4")

        # Add a symlink (should be ignored)
        (dir2 / "link.txt").symlink_to(dir1 / "file1.txt")

        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()

        return subject_dir, output_dir

    def test_basic_generation(self, test_dir):
        subject_dir, output_dir = test_dir
        generate_manifests(str(subject_dir), str(output_dir))

        manifest1_path = output_dir / "dir1.json"
        manifest2_path = output_dir / "dir2.json"

        assert manifest1_path.exists()
        assert manifest2_path.exists()

        with open(manifest1_path) as f:
            data1 = json.load(f)

        # dir1 has 3 files (file1.txt, file2.csv, subdir/file3.txt)
        assert len(data1["files"]) == 3
        paths = [r["path"] for r in data1["files"]]
        assert str(Path("dir1/file1.txt")) in paths
        assert str(Path("dir1/file2.csv")) in paths
        assert str(Path("dir1/subdir/file3.txt")) in paths

        with open(manifest2_path) as f:
            data2 = json.load(f)
        # dir2 has 1 file (file4.txt), link.txt should be ignored
        assert len(data2["files"]) == 1
        assert data2["files"][0]["path"] == str(Path("dir2/file4.txt"))
        assert data2["files"][0]["name"] == "file4.txt"

    def test_regex_filtering(self, test_dir):
        subject_dir, output_dir = test_dir
        # Only include .txt files, but exclude file1.txt
        generate_manifests(str(subject_dir), str(output_dir), include_regex=".*\\.txt$", exclude_regex="file1\\.txt")

        with open(output_dir / "dir1.json") as f:
            data1 = json.load(f)

        # Should have file3.txt but not file1.txt (excluded) or file2.csv (not matched by include)
        paths = [r["path"] for r in data1["files"]]
        assert str(Path("dir1/subdir/file3.txt")) in paths
        assert str(Path("dir1/file1.txt")) not in paths
        assert str(Path("dir1/file2.csv")) not in paths
        assert len(data1["files"]) == 1

    def test_checksum_and_size(self, test_dir):
        subject_dir, output_dir = test_dir
        generate_manifests(str(subject_dir), str(output_dir), include_checksum=True, include_size=True)

        with open(output_dir / "dir1.json") as f:
            data1 = json.load(f)

        file1_record = next(r for r in data1["files"] if r["name"] == "file1.txt")
        assert "md5sum" in file1_record
        assert "size" in file1_record
        assert file1_record["size"] == len("content1")
        import hashlib
        assert file1_record["md5sum"] == hashlib.md5("content1".encode()).hexdigest()

    def test_empty_directory(self, test_dir):
        subject_dir, output_dir = test_dir
        empty_dir = subject_dir / "empty_dir"
        empty_dir.mkdir()

        generate_manifests(str(subject_dir), str(output_dir))

        manifest_path = output_dir / "empty_dir.json"
        assert not manifest_path.exists()
