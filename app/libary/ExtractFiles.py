import zipfile

class ExtractFiles():
    def __init__(self, zip_file_path:str, extract_dir:str):
        self.zip_file_path = zip_file_path
        self.extract_dir = extract_dir
    def zip_extract(self):
        try:
            with zipfile.ZipFile(self.zip_file_path, 'r') as zip_ref:
                return zip_ref.extractall(self.extract_dir)
        except:
            print("Error extracting files.")
            return False