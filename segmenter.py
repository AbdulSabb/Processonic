import os
import sys
from joblib import Parallel, delayed

class Segmenter():
    pass





    # numbers = [1, 2, 3, 3, 5, 3]
    # numbers = [7, 3, 2, 8, 1, 2, 7, 4]
    # threshold = 8
    # numbers2 = list(reversed(sorted(numbers)))
    # i = 0
    # segmented_array = []
    # segment = []
    # fsh8 = 0
    #
    # def append_segment(delete=True):
    #     global fsh8, i, segmented_array, segment, numbers2
    #     segmented_array.append(segment.copy())
    #     segment.clear()
    #     if delete:
    #         del numbers2[i]
    #     i -= fsh8
    #     fsh8 = 0
    #
    # while i < len(numbers2):
    #     if numbers2[i] + sum(segment) <= threshold:
    #         segment.append(numbers2[i])
    #         if sum(segment) == threshold or i == len(numbers2) - 1:
    #             append_segment()
    #         else:
    #             del numbers2[i]
    #         continue
    #     if i == len(numbers2) - 1:
    #         append_segment(delete=False)
    #     i += 1
    #     fsh8 += 1
    #


# # TODO: Construct a new algorithm
# def segmenter(array, threshold):  # TODO: Check threshold exception, if a file is small
#     array = list(reversed(sorted(array)))
#     segmented_array = []
#     segment = []
#     global jump_back, i
#     i = 0
#     jump_back = 0
#
#     def append_segment(delete=True):
#         global jump_back, i
#         segmented_array.append(segment.copy())
#         segment.clear()
#         if delete:
#             del array[i]
#         i -= jump_back
#         jump_back = 0
#
#     while i < len(array):
#         if array[i].size + sum(segment) <= threshold:
#             segment.append(array[i])
#             if sum(segment) == threshold or i == len(array) - 1:
#                 append_segment()
#             else:
#                 del array[i]
#             continue
#         if i == len(array) - 1:
#             append_segment(delete=False)
#         i += 1
#         jump_back += 1
#
#     if [] in segmented_array: segmented_array.remove([])
#     return segmented_array

#
# def search(array, start_idx=0, end_idx=5, search_val=1):
#     if len(array) !=0:
#         if start_idx == end_idx:
#             return start_idx if array[start_idx] <= search_val else -1
#
#         mid_idx = start_idx + int((end_idx - start_idx) / 2);
#
#         if search_val < array[mid_idx]:
#             return search(array, start_idx, mid_idx, search_val)
#
#         ret = search(array, mid_idx+1, end_idx, search_val)
#         return mid_idx if ret == -1 else ret
#     else:
#         return -1
#
# array = [7,3,2,8,1,2,7,4]
# array.sort()
# threshold = 8
# i = len(array) - 1
# segmented_array = []
# segment = []
# while i >= 0:
#     element = array[i]
#     segment.append(element)
#     del array[i]
#     complement = threshold - element
#     c_idx = search(array, end_idx=len(array)-1, search_val = complement)
#     while c_idx >= 0:
#         segment.append(array[c_idx])
#         del array[c_idx]
#         element = sum(segment)
#         complement = threshold - element
#         c_idx = search(array, end_idx=len(array)-1, search_val = complement)
#     segmented_array.append(segment.copy())
#     segment.clear()
#     i = len(array) - 1
#
# print(segmented_array)
#










