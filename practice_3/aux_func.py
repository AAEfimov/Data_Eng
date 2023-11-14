
import os
from zipfile import ZipFile

def get_next_file_from_zip(zip_arch, fp):
    with ZipFile(zip_arch, 'r') as zf:
        ext_dir = fp + "/extract"
        if not os.path.exists(ext_dir):
            os.mkdir(ext_dir)
        
        for f in zf.infolist():
            zf.extract(f.filename, ext_dir)
            ext_filename = os.path.join(ext_dir, f.filename)

            yield ext_filename

def add_to_dict(m, fdict):
    m = m.lower()
    if m in fdict:
        fdict[m] += 1
    else:
        fdict.setdefault(m, 1)

def get_mult(mult):
    retv = 1
    if mult == 'million':
        retv = 10 ** 6
    elif mult == 'billion':
        retv = 10 ** 9
    else:
        print("UNK mul", mult)
    return retv
