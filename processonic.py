import itertools
import os
import sys
import time
from pathlib import Path
import shutil
from joblib import Parallel, delayed
from py import process
from tqdm import tqdm
import numpy as np


ARCHIVE_FORMAT = 'zip'


class File:
    """
        A class used to represent a File in the system.

        ...

        Attributes
        ----------
        __name : str
            The name of the file with its extension without its path.
        __size : float
            The size of the file in bytes

        __path : str
            The absolute path of the file in the operating system.

    """

    def __init__(self, name, size, path):
        """
        :param name: str
            The name of the file with its extension without its path.

        :param size: int
            The size of the file in bytes

        :param path: str
            The absolute path of the file in the operating system.

        """
        try:
            if len(name) == 0:
                print("File name should have a length greater than zero")
                raise ValueError
            self.__name = name
        except:
            print("There is an error with the name of the file")

        try:
            if type(size) != int:
                print("Size should be an integer or a float")
                raise TypeError
            elif size < 0:
                print("Size cannot be negative")
                raise ValueError
            self.__size = size
        except:
            print("There is an error with the size of the file")

        try:
            if type(path) != str:
                print("The path should be a string")
                raise TypeError
            elif not path_exists(path):
                print("No file exists in the given path")
                raise FileNotFoundError
            self.__path = path
        except:
            print("There is an error with the path of the file")

    def __repr__(self):
        return f"(file name: '{self.__name}', file size: '{self.__size}', file path: '{self.__path}')\n"

    def __gt__(self, other):
        return self.__size > other.get_size()

    def __ge__(self, other):
        return self.__size >= other.get_size()

    def __lt__(self, other):
        return self.__size < other.get_size()

    def __radd__(self, other):
        return self.__size + other

    def get_name(self):
        return self.__name

    def get_size(self):
        return self.__size

    def get_path(self):
        return self.__path


def lowerbound_binary_search(array, start_idx, end_idx, search_val):
    """
    Returns the index of the lowerbound value in a given array. -1 if not found.

    :param array: list
        A list of file objects.

    :param start_idx: int
        The starting index of lowerbound binary search algorithm.

    :param end_idx: int
        The ending index of lowerbound binary search algorithm.

    :param search_val: float
        The lowerbound value to search for in the array.

    :return: int
        The index of the lowerbound value in a given array. -1 if not found.
    """

    if len(array) != 0:
        if start_idx == end_idx:
            return start_idx if array[start_idx].get_size() <= search_val else -1

        mid_idx = start_idx + int((end_idx - start_idx) / 2)

        if search_val < array[mid_idx].get_size():
            return lowerbound_binary_search(array, start_idx, mid_idx, search_val)

        ret = lowerbound_binary_search(array, mid_idx + 1, end_idx, search_val)
        return mid_idx if ret == -1 else ret
    else:
        return -1


def path_exists(path):
    """
    Returns True if the path exists in the operating system, otherwise False.

    :param path: str
        The absolute path of the file in the operating system.
    :return: bool
        The boolean value of whether the path exists in the operating system or not.
    """
    return os.path.exists(path)


def access_directory(path):
    """
    Returns a dictionary containing a directory's list of files, the files' parent name, and the files' parent absolute
    path.

    :param path: str
        The absolute path of the directory in the operating system.
    :return: dict
        A dictionary containing three keys:
        :key 'files': list
            A list of File objects of the children of the directory of the given path.
        :key 'parent_name': str
            The name of the directory (the parent of the files).
        :key 'parent_path: str
            The absolute path of the directory (the parent of the files).
    """
    path = os.path.abspath(path)
    assert os.path.exists(path), "Directory was not found at , " + str(path)
    files_in_directory = [File(name=file.name, size=file.stat().st_size, path=file.path) for file in os.scandir(path)]
    return {'files': files_in_directory, 'parent_name': Path(path).name, 'parent_path': path}


def move_file(source, destination):
    """
    Moves a file from a specified source path to a specified destination path.

    :param source: str
        The absolute source path of the file in the operating system.
    :param destination: str
        The absolute destination path for the file in the operating system.
    :return: None
    """
    shutil.move(source, destination)


def move_files(source, destination):
    """
    Performs move_file(source, destination) on many files inside a directory through a specified source path and
    moves them into a specified destination path.

    :param source: str
        The absolute source path of the directory in the operating system.
    :param destination: str
        The absolute destination path for the file in the operating system.
    :return: None
    """
    directory = access_directory(source)
    for file in directory['files']:
        move_file(file.get_path(), destination)


def make_archive(path, format):
    """
    Archives a file or a directory from and to a specified path, using a given archive format.

    :param path: str
        The absolute source path of the directory in the operating system.
    :param format: str
        The archive format. Archive formats are:  'zip', 'tar', 'gztar', 'bztar', and 'xztar'.
    :return: None
    """
    archive_from = os.path.dirname(path)
    archive_to = os.path.basename(path.strip(os.sep))
    shutil.make_archive(path, format, archive_from, archive_to)


def unpack_archive(source, destination, format):
    """
    Unpacks an archived file from and to the specified path, using a given archive format.

    :param source: str
        The absolute source path of the file in the operating system.
    :param destination: str
        The absolute destination path for the file to be unpacked in the operating system.
    :param format: str
        The archive format. Archive formats are:  'zip', 'tar', 'gztar', 'bztar', and 'xztar'.
    :return: None
    """
    shutil.unpack_archive(source, destination, format)


def unpack_archives(source, destination, format):
    """
    Performs unpack_archive(path, format) on many files inside a directory of a given source path.

    :param source: str
        The absolute source path of the directory with archived files in the operating system.
    :param destination: str
        The absolute destination path for the files to be unpacked in the operating system.
    :param format: str
        The archive format. Archive formats are:  'zip', 'tar', 'gztar', 'bztar', and 'xztar'.
    :return: None
    """
    directory = access_directory(source)
    for file in directory['files']:
        unpack_archive(file.get_path(), destination, format)
        remove_file(file.get_path())


def remove_file(path):
    """
    Removes a file from the operating system using a specified path.

    :param path: str
        The absolute source path of the file in the operating system.
    :return: None
    """
    os.remove(path)


def remove_directory(path):
    """
    Removes all of the files inside a directory recursively using a specified path.

    :param path: str
        The absolute path of the directory in the operating system.
    :return: None
    """
    shutil.rmtree(path)


def make_directory(path):
    """
    Makes a directory in the operating system using a specified path.

    :param path: str
        The absolute path of the directory to be made in the operating system.
    :return: None
    """
    os.mkdir(path)


def make_directories(path, directories):
    """
    Makes directories inside a directory of a given path using a list of directory names.

    :param path: str
        The absolute path of the directory in the operating system.
    :param directories: list(str)
        A list of directory names.
    :return: None
    """
    for directory in directories:
        make_directory(f"{path}/{directory}")


def remove_files(path, suffix=None):
    """
    Removes the files inside a directory of a given path without removing the directory itself. File suffix can be
    specified to only remove files of the corresponding suffix/extension.

    :param path: str
        The absolute path of the directory in the operating system.
    :param suffix: str
        A file suffix/extension without the dot at the beginning.
    :return: None
    """
    directory = access_directory(path)
    for file in directory['files']:
        if suffix:
            if is_equal_suffix(file.get_path(), suffix):
                remove_file(file.get_path())
        else:
            remove_file(file.get_path())



def is_dir_empty(path):
    """
    Checks if a directory is empty by confirming if its length is equal to 0.

    :param path: str
        The absolute path of the directory in the operating system.
    :return: bool
        The boolean value of whether the directory is empty or not.
    """
    return os.path.exists(path) and len(os.listdir(path)) == 0


def is_equal_suffix(path, suffix):
    """
    Returns the boolean value of which if the file of the given path has the same suffix as the specified suffix.

    :param path: str
        The absolute path of the file in the operating system.
    :param suffix: str
        A file suffix/extension without the dot at the beginning.
    :return: bool
        The boolean value of which if the file of the given path has the same suffix as the specified suffix.
    """
    return get_file_suffix(path)[1:] == suffix


def get_file_suffix(path):
    """
    Returns the suffix/extension of the file using a specified path.

    :param path: str
        The absolute path of the file in the operating system.
    :return: str
        The suffix/extension of the file.
    """
    try:
        if not path_exists(path):
            print("No file exists in the given path")
            raise FileNotFoundError
        elif not is_dir(path) and not is_file(path):
            print("The item of the given path is not accessible")
            raise TypeError
        return Path(path).suffix
    except:
        print("There is an error with the given path")


def is_file(path):
    """
    Returns a boolean value of which if the item in the given path is a file or not.

    :param path: str
        The absolute path of the item in the operating system.
    :return: bool
        The boolean value of which if the item is a file or not.
    """
    return Path(path).is_file()


def is_dir(path):
    """
    Returns a boolean value of which if the item in the given path is a directory or not.

    :param path: str
        The absolute path of the item in the operating system.
    :return: bool
        The boolean value of which if the item is a directory or not.
    """
    return Path(path).is_dir()


def move_children_up(path):
    """
    Moves the children of a directory to its parent directory.

    :param path: str
        The absolute path of the directory in the operating system.
    :return: None
    """
    directory = access_directory(path)
    for subdir in directory['files']:
        move_files(subdir.get_path(), directory['parent_path'])
        remove_directory(subdir.get_path(),)


def chunk_file(file, extension, path, threshold):
    """
    Chunks the file into chunks of binary files with each chunk having an upperbound size limit as the threshold.

    :param file: .bin file
        A binary representation of the original file to be chunked
    :param extension: str
        The original file's extension
    :param path: str
        The absolute path of the original file in the operating system.
    :param threshold: int
        The upperbound/threshold of the file size in bytes. Chunks are done based on it.
    :return: None
    """
    read_buffer_size = 1024
    chunk_size = threshold
    current_chunk_size = 0
    current_chunk = 1
    done_reading = False
    while not done_reading:
        with open(f'{path}{current_chunk}{extension}.chk', 'ab') as chunk:
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


def split_file(path, threshold):
    """
    Splits the file in the giving path into chunks each having an upperbound size limit as the threshold.

    :param path: str
        The absolute path of the original file in the operating system.
    :param threshold: int
        The upperbound/threshold of the file size in bytes. Chunks are done based on it.
    :return: None
    """
    p = Path(path)
    file_to_split = None
    if p.is_file() and p.name[0] != '.':
        file_to_split = p

    if file_to_split:
        with open(file_to_split, 'rb') as file:
            chunk_file(file, file_to_split.suffix, path, threshold)
        remove_file(path)


def split_files(path, threshold):
    """
    Performs split_file(path, threshold) on many files inside a directory through a specified path and based on an
    upperbound size limit as the threshold.

    :param path: str
        The absolute path of the directory in the operating system.
    :param threshold: int
        The upperbound/threshold of the file size in bytes.
    :return: None
    """
    directory = access_directory(path)
    for file in directory['files']:
        if file.get_size() > threshold:
            split_file(file.get_path(), threshold)


def get_chunks_dict(path):
    """
    Returns a dictionary with keys as original file names, and values as lists of chunk paths for each original file from
    a specific directory through a specified path.

    :param path: str
        The absolute path of the directory in the operating system.
    :return: dict
        A dictionary of chunks for each original file:
        :key: str
            The file name in its original form with no extension.
        :value: list(Path)
            A list of paths of the chunks of the original file name.
    """
    def get_sorting_index(path):
        """
        Returns the index for which the sorting should depend on.

        :param path: Path
            The path object of the file in the operating system.
        :return: int
            The index for which the sorting should depend on.
        """
        return int(path.suffixes[-3][len(path.suffixes[-2]):])

    chunks_dict = {}
    directory = Path(path)
    chunks = list(directory.rglob('*.chk'))
    chunks.sort(key=lambda path: get_sorting_index(path))
    for chunk in chunks:
        first_extension = chunk.suffixes[-3]
        partition = chunk.name.find(first_extension)
        file_name = chunk.name[:partition]
        if file_name in chunks_dict.keys():
            chunks_dict[file_name].append(chunk)
        else:
            chunks_dict[file_name] = [chunk]

    return chunks_dict


def join_file(file_name, chunks):
    """
    Joins the chunks of a file together into its original form.

    :param file_name: str
        The name of the original file in its original form with no extension.
    :param chunks: list(Path)
         A list of paths of the chunks of the original file name.
    :return: None
    """
    read_buffer_size = 1024
    extension = chunks[0].suffixes[-2]
    parent_path = chunks[0].parent
    with open(f'{parent_path}/{file_name}{extension}', 'ab') as file:
        for chunk in chunks:
            with open(chunk, 'rb') as piece:
                while True:
                    bfr = piece.read(read_buffer_size)
                    if not bfr:
                        break
                    file.write(bfr)
            remove_file(chunk)


def join_files(path):
    """
    Performs join_file(file_name, chunks) on many files inside a directory through a specified path.

    :param path: str
        The absolute path of the directory in the operating system.
    :return: None
    """
    chunks_dict = get_chunks_dict(path)
    for file_name, chunks in chunks_dict.items():
        join_file(file_name, chunks)


def segmenter(array, threshold):
    """
    Returns a segmented array of File objects based on a given threshold to the minimal number of segments possible.

    :param array: list(File)
        A list of File objects containing files in one directory.
    :param threshold: int
        The upperbound/threshold of the segment size in bytes.
    :return: list(File)
        A segmented array of File objects.
    """
    array.sort()
    i = len(array) - 1
    segmented_array = []
    segment = []
    while i >= 0:
        element = array[i].get_size()
        segment.append(array[i])
        del array[i]
        i -= 1
        complement = threshold - element
        c_idx = lowerbound_binary_search(array, start_idx=0, end_idx=len(array) - 1, search_val=complement)
        while c_idx >= 0:
            segment.append(array[c_idx])
            del array[c_idx]
            i -= 1
            element = sum(segment)
            complement = threshold - element
            c_idx = lowerbound_binary_search(array, start_idx=0, end_idx=len(array) - 1, search_val=complement)
        segmented_array.append(segment.copy())
        segment.clear()
    return segmented_array


def segment_directory(path, threshold):
    """
    Segments a directory of a given path based on an upperbound size limit as the threshold. Each segment will create
    a subdirectory with the name of the original directory plus an index.

    :param path: str
        The absolute path of the directory in the operating system.
    :param threshold: int
        The upperbound/threshold of each file's size in bytes.
    :return: None
    """
    if not is_dir_empty(path):
        split_files(path, threshold)
        directory = access_directory(path)
        for index, dir in enumerate(segmenter(directory['files'], threshold)):
            new_subdir_name = f'/{directory["parent_name"]}_{index}'
            parent_path = directory['parent_path']
            source = parent_path + new_subdir_name
            make_directory(source)
            for file in dir:
                destination = f"{source}/{file.get_name()}"
                move_file(file.get_path(), destination)
            make_archive(source, ARCHIVE_FORMAT)
            remove_directory(source)
    else:
        directory = access_directory(path)
        new_subdir_name = f'/{directory["parent_name"]}_0'
        parent_path = directory['parent_path']
        source = parent_path + new_subdir_name
        make_directory(source)
        make_archive(source, ARCHIVE_FORMAT)
        remove_directory(source)


def get_subdirs_dict(source):
    """
    Returns a dictionary with keys as the original directories' names, and values as lists of File objects representing
    the segmented archived subdirectories.

    :param source: str
        The absolute source path of the directory of archived files in the operating system.
    :return: dict
        A dictionary of segmented archived subdirectories for each original directory:
        :key: str
            The directory name in its original form.
        :value: list(File)
            A list of File object of the segmented archived subdirectories of the original dictionary.
    """
    directories = {}
    directory = access_directory(source)
    for file in directory['files']:
        partition = file.get_name().rfind('_')
        folder_name = file.get_name()[:partition]
        if folder_name in directories.keys():
            directories[folder_name].append(file)
        else:
            directories[folder_name] = [file]
    return directories


def distribute_subdirs(directories, destination):
    """
    Distributes the archived files in the given destination path back to their original subdirectory place.

    :param directories: dict
        A dictionary of segmented archived subdirectories for each original directory:
        :key: str
            The directory name in its original form.
        :value: list(File)
            A list of File object of the segmented archived subdirectories of the original dictionary.
    :param destination: str
        The absolute destination path of the directory of subdirectories in the operating system.
    :return: None
    """
    for folder, files in directories.items():
        for file in files:
            move_file(file.get_path(), f"{destination}/{folder}")


def task_one_single(source, destination, threshold):
    """
    Performs segment_directory(path, threshold) on a directory of the given source path, then moves the segmented
    archived files to the specified destination path. This is done only on a single directory.

    :param source: str
        The absolute source path of the directory in the operating system.
    :param destination: str
        The absolute destination path for the directory in the operating system.
    :param threshold: int
        The upperbound/threshold of the a file's size in bytes.
    :return: None
    """

    segment_directory(source, threshold)
    move_files(source, destination)
    remove_directory(source)


def task_one(source, destination, threshold):
    """
    Performs task_one_single(source, destination, threshold) on many subdirectories inside a directory of the given
    source path.

    :param source: str
        The absolute source path of the directory in the operating system.
    :param destination: str
        The absolute destination path for the directory in the operating system.
    :param threshold: int
        The upperbound/threshold of the a file's size in bytes.
    :return: None
    """
    directory = access_directory(source)
    progress = tqdm(np.arange(len(directory['files'])), desc="Loading")
    for subdir in directory['files']:
        task_one_single(subdir.get_path(), destination, threshold)
        progress.update()


def task_two(source, destination):
    """
    Distributes the archived segmented files in the given source path directory back to their original place, and then
    unpack these archived files and get them back to their original form as they were before and joins the split files
    if exist.

    :param source: str
        The absolute source path of the directory containing the archived segmented files in the operating system.
    :param destination: str
        The absolute destination path for the directory in the operating system.
    :return: None
    """
    directories = get_subdirs_dict(source)
    make_directories(destination, directories.keys())
    distribute_subdirs(directories, destination)
    directory = access_directory(destination)
    progress = tqdm(np.arange(len(directory['files'])), desc="Loading")
    for subdir in directory['files']:
        subdir_path = subdir.get_path()
        unpack_archives(subdir_path, subdir_path, ARCHIVE_FORMAT)
        remove_files(subdir_path, ARCHIVE_FORMAT)
        move_children_up(subdir_path)
        join_files(subdir_path)
        progress.update()


task_one("D:\Xina\Test\TestAA", "D:\movehere", 100000)
task_two("D:\movehere", "D:\Xina\Test\TestAA")

# TODO: Check if directory is already made
