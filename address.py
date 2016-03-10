def address(key, filestate):
    bucket = key % 2**filestate.level
    if bucket < filestate.splitPtr:
        bucket = key % 2**(filestate.level+1)
    return bucket

