import zipfile


def unzip_file(zip_path, save_path):
    ''' Распаковывает zip-архив в заданную папку
    '''
    print(f"Unzipping {zip_path} to {save_path}")
    with zipfile.ZipFile(zip_path) as zip_file:
        for names in zip_file.namelist():
            zip_file.extract(names, save_path)
