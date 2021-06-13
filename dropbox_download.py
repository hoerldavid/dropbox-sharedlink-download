import pathlib2
import hashlib
import os, sys
import argparse
import json
from functools import reduce
from operator import add
from concurrent.futures import ThreadPoolExecutor

import dropbox
import tqdm


CHUNK_SIZE = 4 * 1024 * 1024 
def db_content_hash(file):
    '''
    get Dropbox content hash of an already downloaded file as described on:
    https://www.dropbox.com/developers/reference/content-hash
    '''
    with open(file, 'rb') as fd:
        chunk_hashes = b''
        while(chunk := fd.read(CHUNK_SIZE)):
            chunk_hashes += hashlib.sha256(chunk).digest()
    return hashlib.sha256(chunk_hashes).hexdigest()


def get_file_list_recursive(link_root, api_token, file_list_location):

    print('--- Getting list of files ---')

    if os.path.exists(file_list_location):
        with open(file_list_location) as fd:
            print(f'Loading saved file list from {file_list_location}.')
            all_files = json.load(fd)['file_list']
        return all_files
        
    with dropbox.Dropbox(api_token) as dbx:

        link = dropbox.files.SharedLink(link_root)

        all_files = []

        path_worklist = ['']

        while not len(path_worklist) < 1:
            path = path_worklist.pop()

            print(f'Investigating path {path}.')

            # get first batch of folder entries
            res = dbx.files_list_folder(path, shared_link=link)
            entries = res.entries

            # optionally get additional batches
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)

            for entry in entries:
                # recurse for folders
                if isinstance(entry, dropbox.files.FolderMetadata):
                    path_worklist.append('/'.join([path, entry.name]))
                # add files to download list
                elif isinstance(entry, dropbox.files.FileMetadata):
                    all_files.append((path, entry.name, entry.content_hash, entry.size))

    if file_list_location is not None:
        filelist_json = {'shared_link': link_root, 'file_list': all_files}
        with open(file_list_location, 'w') as fd:
            json.dump(filelist_json, fd, indent=1)

    return all_files

def download_file_list(files_to_download, api_token, target_dir, link_root, n_parallel=1):
           
    with dropbox.Dropbox(api_token) as dbx, ThreadPoolExecutor(n_parallel) as pool:

        futures = []

        print('--- Checking for already downloaded files and preparing folders')
        for path, name, content_hash, _ in tqdm.tqdm(files_to_download):

            target_file = pathlib2.Path(target_dir) / pathlib2.Path(path.strip('/')) / pathlib2.Path(name)

            # check if target dir exists
            if not os.path.exists(target_file.parent):
                os.makedirs(target_file.parent)

            # if file exists, check hash and skip if we already have it
            if os.path.exists(target_file):
                if db_content_hash(target_file) == content_hash:
                    continue

            # do download
            futures.append(pool.submit(dbx.sharing_get_shared_link_file_to_file, str(target_file), link_root, '/'.join([path, name ])))
            # dbx.sharing_get_shared_link_file_to_file(str(target_file), link_root, '/'.join([path, name ]))
        
        print(f'Downloading the remaining {len(futures)} files in {n_parallel} threads')
        for future in tqdm.tqdm(futures):
            future.result()


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Download Dropbox shared link recursively')
    parser.add_argument('shared_link', type=str, help='Dropbox shared link to recursively download')
    parser.add_argument('--target_dir', type=str, default=os.getcwd(), help='Directory to save downloaded files to')
    parser.add_argument('--api_token_file', type=str, default='secrets.json', help='Dropbox API token location')
    parser.add_argument('--file_list_location', type=str, help='Where to save list of files to download')
    parser.add_argument('--n_parallel', type=int, default=1, help='How many downloads to do in parallel')
    parser.add_argument('--max_num_files', type=int, help='Debug: only download the first N files in list')

    args = parser.parse_args()

    target_dir = args.target_dir
    link_root = args.shared_link

    if not os.path.exists(args.api_token_file):
        sys.exit('API token file not found')

    with open(args.api_token_file) as fd:
        # api_token_file must contain a single key-value pair 'api_token': Dropbox API token
        # needs permissions sharing.read and maybe files.content.read
        # make sure to set the token to not expire for very large downloads
        api_token = json.load(fd)['api_token']

    # default = None -> download everything   
    max_num_files = args.max_num_files

    all_files = get_file_list_recursive(link_root, api_token, args.file_list_location)

    if max_num_files is not None:
        files_to_download = all_files[:max_num_files]
    else: files_to_download = all_files

    print(f'Will download {len(files_to_download)} file(s) ({reduce(add, [s for _,_,_,s in files_to_download])} bytes) to {target_dir}')

    download_file_list(files_to_download, api_token, target_dir, link_root, args.n_parallel)