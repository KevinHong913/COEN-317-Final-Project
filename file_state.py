from math import log


class FileState(object):
    def __init__(self, extent=1):
        self.extent = extent
        self.level = int(log(extent, 2))
        self.splitPtr = self.extent - 2**self.level

    def __repr__(self):
        return "level={}; splitPtr={}".format(self.level, self.splitPtr)

#     def split(self):
#         oldBucket = self.splitPtr
#         newBucketNbr = self.splitPtr + 2**self.level
#         self.splitPtr += 1
#         if self.splitPtr == 2**self.level:
#             self.level += 1
#             self.splitPtr = 0
# 
#     def address(self, key):
#         bucket = key % 2**self.level
#         if bucket < self.splitPtr:
#             bucket = key % 2**(self.level+1)
#         return bucket
