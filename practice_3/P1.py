

from zipfile import ZipFile
import os

z_numb = 1
zip_file_name = 'zip_var_40.zip'
file_path = 'tests/' + str(z_numb)

filename = file_path + '/' + zip_file_name

with ZipFile(filename, 'r') as zf:
    zf.printdir()
    print('Done')


