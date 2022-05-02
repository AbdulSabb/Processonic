import itertools
import os
import pathlib
import sys
import time
from pathlib import Path
import shutil
from joblib import Parallel, delayed
from py import process
from tqdm import tqdm
import numpy as np


def lowerbound_binary_search(array, start_idx=0, end_idx=5, search_val=1):  #
    if len(array) != 0:
        if start_idx == end_idx:
            return start_idx if array[start_idx].size <= search_val else -1

        mid_idx = start_idx + int((end_idx - start_idx) / 2);

        if search_val < array[mid_idx].size:
            return lowerbound_binary_search(array, start_idx, mid_idx, search_val)

        ret = lowerbound_binary_search(array, mid_idx + 1, end_idx, search_val)
        return mid_idx if ret == -1 else ret
    else:
        return -1


def split_big_files(abspath, threshold):  #
    folder = access_folder(abspath)['files']
    for file in folder:
        if file.size > threshold:
            split(file.path, threshold)


def segmenter(array, threshold):  #
    array.sort()
    i = len(array) - 1
    segmented_array = []
    segment = []
    while i >= 0:
        element = array[i].size
        segment.append(array[i])
        del array[i]
        i -= 1
        complement = threshold - element
        c_idx = lowerbound_binary_search(array, end_idx=len(array) - 1, search_val=complement)
        while c_idx >= 0:
            segment.append(array[c_idx])
            del array[c_idx]
            i -= 1
            element = sum(segment)
            complement = threshold - element
            c_idx = lowerbound_binary_search(array, end_idx=len(array) - 1, search_val=complement)
        segmented_array.append(segment.copy())
        segment.clear()
    return segmented_array


class File:  #
    def __init__(self, name, size, path):
        self.name = name
        self.size = size
        self.path = path

    def __repr__(self):
        return f"(file name: '{self.name}', file size: '{self.size}', file path: '{self.path}')\n"

    def __gt__(self, other):
        return self.size > other.size

    def __ge__(self, other):
        return self.size >= other.size

    def __lt__(self, other):
        return self.size < other.size

    def __radd__(self, other):
        return self.size + other


def get_sizes(file_objects):
    return [file.size for file in file_objects]


def access_folder(abspath): #
    path = os.path.abspath(abspath)
    assert os.path.exists(path), "I did not find the file at, " + str(abspath)
    files_in_folder = [File(name=file.name, size=file.stat().st_size, path=file.path) for file in os.scandir(abspath)]
    return {'files': files_in_folder, 'parent_name': Path(abspath).name, 'parent_path': abspath}


def is_folder_empty(folder_path):
    """Check if folder is empty by confirming if its length is 0"""
    # Check if file exist and it is empty
    return os.path.exists(folder_path) and len(os.listdir(folder_path)) == 0

def file_exists(path):  #
    return os.path.exists(path)

def make_archive(source): #
    name = source
    archive_from = os.path.dirname(source)
    archive_to = os.path.basename(source.strip(os.sep))
    shutil.make_archive(name, 'zip', archive_from, archive_to)


def make_directory(path):  #
    os.mkdir(path)


def segment_folder(abspath, threshold):  #
    split_big_files(abspath, threshold)
    folder_dict = access_folder(abspath)
    folder = folder_dict['files']
    for index, folder in enumerate(segmenter(folder, threshold)):
        new_subfolder_name = f'\{folder_dict["parent_name"]}_{index}'
        parent_path = folder_dict['parent_path']
        temp_parent_path = parent_path + new_subfolder_name
        source = temp_parent_path
        make_directory(source)
        for file in folder:
            destination = f"{temp_parent_path}\{file.name}"
            move_file(file.path, destination)
        make_archive(source)
        remove_directory(source)


def remove_directory(source):  #
    shutil.rmtree(source)


def remove_file(source):  #
    os.remove(source)


def remove_files_from_folder(folder_path):  #
    folder = access_folder(folder_path)
    for file in folder['files']:
        if is_equal_suffix(file.path, 'zip'):
            remove_file(file.path)


def move_file(source, destination):  #
    shutil.move(source, destination)


def move_files_from_folder(folder_source, destination):  #
    folder = access_folder(folder_source)
    for file in folder['files']:
        move_file(file.path, destination)


def segment_folders(source, destination, threshold):  #
    segment_folder(source, threshold)
    move_files_from_folder(folder_source=source, destination=destination)


def get_merged_subfolders_dict(source):
    folders_dict = {}
    folder = access_folder(source)
    for file in folder['files']:
        partition = file.name.rfind('_')
        folder_name = file.name[:partition]

        if folder_name in folders_dict.keys():
            folders_dict[folder_name].append(file)
        else:
            folders_dict[folder_name] = [file]
    return folders_dict


def merge_subfolders_to_folder(folders_dict, folder_destination):
    for folder, files in folders_dict.items():
        for file in files:
            move_file(source=file.path, destination=f"{Path(folder_destination).parent}\{folder}")


def unpack_archive(source, destination):  #
    shutil.unpack_archive(source, destination, 'zip')


def unpack_archives(folder_source, destination):  #
    folder = access_folder(folder_source)
    for file in folder['files']:
        unpack_archive(file.path, destination)
        remove_file(file.path)


def get_file_suffix(path):  #
    return pathlib.Path(path).suffix


def is_equal_suffix(path, suffix):  #
    return get_file_suffix(path)[1:] == suffix


def move_children_up(folder_path):  #
    directory = access_folder(folder_path)
    for folder in directory['files']:
        move_files_from_folder(folder.path, directory['parent_path'])
        remove_directory(folder.path)


def task_one_single(source, destination, threshold):
    segment_folder(source, threshold)
    move_files_from_folder(folder_source=source, destination=destination)


def task_two_single(source, destination):
    folders_dict = get_merged_subfolders_dict(destination)
    merge_subfolders_to_folder(folders_dict, source)
    unpack_archives(source, source)
    remove_files_from_folder(source)
    move_children_up(source)
    join_many(source)


def task_one(source, destination, threshold):
    folder = access_folder(source)
    progress = tqdm(np.arange(len(folder['files'])), desc="Loading")
    for file in folder['files']:
        task_one_single(file.path, destination, threshold)
        progress.update()


def task_two(source, destination):
    folder = access_folder(source)
    progress = tqdm(np.arange(len(folder['files'])), desc="Loading")
    for file in folder['files']:
        task_two_single(file.path, destination)
        progress.update()


def chunk_file(file, extension, abspath, size):   #
    read_buffer_size = 1024
    chunk_size = size
    current_chunk_size = 0
    current_chunk = 1
    done_reading = False
    while not done_reading:
        with open(f'{abspath}{current_chunk}{extension}.chk', 'ab', ) as chunk:
            while True:
                bfr = file.read(read_buffer_size)
                if not bfr:
                    done_reading = True
                    break

                chunk.write(bfr)
                current_chunk_size += len(bfr)
                if current_chunk_size + read_buffer_size > chunk_size:
                    current_chunk += 1
                    current_chunk_size = 0
                    break


def split(abspath, size):   #
    p = Path(abspath)
    file_to_split = None
    if p.is_file() and p.name[0] != '.':
        file_to_split = p

    if file_to_split:
        with open(file_to_split, 'rb') as file:
            chunk_file(file, file_to_split.suffix, abspath, size)
    remove_file(abspath)


def get_chunks_dict(abspath):  #
    chunks_dict = {}
    folder = Path(abspath)
    chunks = list(folder.rglob('*.chk'))
    chunks.sort()
    for chunk in chunks:
        first_extension = chunk.suffixes[-3]
        partition = chunk.name.find(first_extension)
        file_name = chunk.name[:partition]
        if file_name in chunks_dict.keys():
            chunks_dict[file_name].append(chunk)
        else:
            chunks_dict[file_name] = [chunk]

    return chunks_dict


def join_many(abspath):  #
    chunks_dict = get_chunks_dict(abspath)
    for file_name, chunks in chunks_dict.items():
        join(file_name, chunks)


def join(file_name, chunks):  #
    read_buffer_size = 1024
    extension = chunks[0].suffixes[-2]
    parent_path = chunks[0].parent
    with open(f'{parent_path}\{file_name}{extension}', 'ab') as file:
        for chunk in chunks:
            with open(chunk, 'rb') as piece:
                while True:
                    bfr = piece.read(read_buffer_size)
                    if not bfr:
                        break
                    file.write(bfr)
            remove_file(chunk)

# split_big_files("D:\Xina\Test\Test54\F4", 5000000)
# print(get_chunks_dict("D:\Xina\Test\Test54\F4"))
#
# join_many("D:\Xina\Test\Test54\F4")
# split("D:\Xina\Test\Test99\F6\Distributed_File_Systems_Concepts_and_Examples.pdf")

# join()

# segment_folder("D:\Xina\Test\Test98\F5", 250000)


# task_one("D:\Xina\Test\Test60", "D:\movehere", 5000000)
# task_two("D:\Xina\Test\Test60", "D:\movehere")


# segment_folder(access_folder(r"D:\Xina\Test\Test8\F5"))
# move_files_from_folder(folder_source="D:\Xina\Test\Test8\F5", destination="D:\movehere")


# unpack_archive("D:\Xina\Test\Test8\F5\F5_0.zip", "D:\Xina\Test\Test8\F5")
# remove_file("D:\Xina\Test\Test8\F5\F5_0.zip")


# files = access_folder(r"D:\Xina\Test\Test3\F6")
#
# print(is_folder_empty("D:\Xina\Test\Test3\F6"))
#
# print()
#
# print(files['parent_path'])
#
# segment_folder(access_folder(r"D:\Xina\Test\Test5\F1"))
