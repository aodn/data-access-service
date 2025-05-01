import unittest
import os
import tempfile
import shutil
from data_access_service.utils.file_utils import zip_the_folder


class TestZipTheFolder(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory and files for testing
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, "test_file.txt")
        with open(self.test_file, "w") as f:
            f.write("This is a test file.")

        self.output_zip = os.path.join(self.test_dir, "output_archive")

    def tearDown(self):
        # Clean up temporary files and directories
        shutil.rmtree(self.test_dir)

    def test_zip_the_folder(self):
        # Call the function to zip the folder
        zip_the_folder(self.test_dir, self.output_zip)

        # Check if the ZIP file is created
        self.assertTrue(os.path.exists(self.output_zip + ".zip"))

        # Verify the contents of the ZIP file
        with tempfile.TemporaryDirectory() as extract_dir:
            shutil.unpack_archive(self.output_zip + ".zip", extract_dir, "zip")
            extracted_file = os.path.join(extract_dir, "test_file.txt")
            self.assertTrue(os.path.exists(extracted_file))
            with open(extracted_file, "r") as f:
                self.assertEqual(f.read(), "This is a test file.")


if __name__ == "__main__":
    unittest.main()
