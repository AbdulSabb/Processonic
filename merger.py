import os
from joblib import Parallel, delayed

class Merger():
    pass


# with open("D:\Xina\Test\Test99\F6\Distributed_File_Systems_Concepts_and_Examples.pdf", 'rb') as src_gz,  open("sink.txt", 'wb') as sink:
#   chunk_size = 1024 * 1024 # 1024 * 1024 byte = 1 mb
#   while True:
#     chunk = src_gz.read(chunk_size)
#     if not chunk:
#       break
#     sink.write(dec.compress(chunk))
#
# import multiprocessing as mp,os
#
# def process_wrapper(chunkStart, chunkSize):
#     with open("D:\Xina\Test\Test99\F6\Distributed_File_Systems_Concepts_and_Examples.pdf") as f:
#         f.seek(chunkStart)
#         lines = f.read(chunkSize).splitlines()
#         for line in lines:
#             process(line)
#
# def chunkify(fname,size=1024*1024):
#     fileEnd = os.path.getsize(fname)
#     with open(fname,'r') as f:
#         chunkEnd = f.tell()
#     while True:
#         chunkStart = chunkEnd
#         f.seek(size,1)
#         f.readline()
#         chunkEnd = f.tell()
#         yield chunkStart, chunkEnd - chunkStart
#         if chunkEnd > fileEnd:
#             break
#
# #init objects
# pool = mp.Pool(4)
# jobs = []
#
# #create jobs
# for chunkStart,chunkSize in chunkify("input.txt"):
#     jobs.append( pool.apply_async(process_wrapper,(chunkStart,chunkSize)) )
#
# #wait for all jobs to finish
# for job in jobs:
#     job.get()
#
# #clean up
# pool.close()
#