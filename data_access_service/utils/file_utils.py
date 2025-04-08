import shutil


def zip_the_folder(folder_path: str, output_zip_path:str):
    shutil.make_archive(output_zip_path, 'zip', folder_path)
