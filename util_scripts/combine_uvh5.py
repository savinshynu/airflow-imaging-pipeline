"""
Given a path of text file  containing multiple UVH5 file paths, combine them into a 
single UVh5 file and convert it to the measurement set format.
"""
import sys
import os
import glob
import shutil
from pyuvdata import UVData

# Path of text file
filepath = sys.argv[1]

# uvh5 directory
uvh5_path = "/data/horse/ws/sash820h-test-corr/prod/uvh5/"

#Out file path
out_path = "/data/horse/ws/sash820h-test-corr/prod/ms/"

with open(filepath, 'r') as fh:
    files = fh.readlines()

if files:
    print("Collecting all the uvh5 files")
    
    # change the colon in time tag to hyphen, otherwise causing binding issues while running singularity
    outfile = out_path + os.path.splitext(os.path.basename(filepath))[0].replace(":", "-")+".ms"
    print(f"Writing out the MS file to {outfile}")
    for filename in files:
        filename = uvh5_path + os.path.splitext(os.path.basename(filename))[0]+".uvh5"
        print(filename)
        if os.path.isfile(filename):
            uvd = UVData()
            uvd.read(filename, fix_old_proj=False, fix_use_ant_pos=False)

            try:
                uvd_main += uvd
            except NameError:
                uvd_main = uvd
   
    if not os.path.isdir(outfile):
        uvd_main.write_ms(outfile)
        
    else:
        # Remove the existing measurement set directory
        print("MS file already exists, overwriting now")
        shutil.rmtree(outfile)
        # Now write the new measurement set to avoid confusion.
        uvd_main.write_ms(outfile)
else:
    raise Exception("No files to concatenate")
        