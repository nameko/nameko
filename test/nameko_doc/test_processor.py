import pytest

from nameko.nameko_doc.processor import ServiceDocProcessor


def empty_loader():
    return []


class TestProcessor(object):
    def test_build_empty_project(self, temp_folder_factory):
        output_folder = temp_folder_factory(reserve_only=True)

        processor = ServiceDocProcessor(
            output_folder, empty_loader
        )
        processor.write_docs()

        assert output_folder.exists()
        assert output_folder.files() == []

    def test_build_into_existing_folder(self, temp_folder_factory):
        output_folder = temp_folder_factory()

        # Add something in there
        output_folder.joinpath('bye.txt').write_text('bye!')

        assert len(output_folder.files()) == 1

        processor = ServiceDocProcessor(output_folder, empty_loader)

        with pytest.raises(ValueError):
            processor.write_docs()
        assert len(output_folder.files()) == 1
