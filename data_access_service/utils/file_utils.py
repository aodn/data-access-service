import shutil


def zip_the_folder(folder_path: str, output_zip_path: str) -> str:
    print("folder path", folder_path)
    print("output zip path", output_zip_path)
    return shutil.make_archive(output_zip_path, "zip", folder_path)
