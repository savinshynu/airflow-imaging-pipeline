import os
import sys
import fnmatch
import json

# directory containing all the dada files
data_dir = '/data/narwhal/usr/projects/p_dza/p_dza_mpifr_compact/DADA_FILES/2024-05-19-15:49:28/UHF/'

# directory to which we write out the output text file
out_dir = '/data/horse/ws/sash820h-test-corr/prod/txt_files/'

def find_all_files(inp_filenames: list) -> str:
    """
    Find all DADA files from different spectral folders and
    write them into a single text file which can be fed for 
    SLURM array jobs

    Input: Filenames of a single dada file from spectral 00 folder (file corresponding to different timestamps)
    Returns: List of ouput text files containing all the corresponding time stamp files from all the spectral folders
    """
    # dictionary to collect all the information.
    log_dict = {'file_meta':[]}

    for inp_filename in inp_filenames:
    
        # spectral folders in the data directory
        dirs = sorted([dir for dir in os.listdir(data_dir) if os.path.isdir(data_dir + dir)])
        #print(dirs)

        out_files = os.path.splitext(inp_filename)[0].split('_00_')
        source_dt = out_files[0]
        tstamp = out_files[1]

        # output text file containing all dada filenames
        out_filename = out_dir + source_dt + '_' + tstamp + '.txt'

        num_files = 0
        with open(out_filename, 'w') as fh:
            
            for dir in dirs:
                search_dir = data_dir + dir
                dada_files = fnmatch.filter(os.listdir(search_dir), source_dt + '*' + tstamp +'.dada')
                if dada_files and len(dada_files) == 1:
                    dada_file = dada_files[0]
                    num_files += 1
                    fh.write(data_dir + dir + '/' + dada_file + '\n')
                else:
                    
                    #print(json.dumps({dir: f"No file found for {tstamp} time stamp in {dir} folder"}))
                    log_dict.update({f"{tstamp}": f"No file in {dir} folder"})
        log_dict['file_meta'].append({'file_path': out_filename, 'num_files': num_files})
    
    print(json.dumps(log_dict))


if __name__ == '__main__':
    
    inp_filenames = sys.argv[1:]
    #print(inp_filenames)
    find_all_files(inp_filenames)
    
