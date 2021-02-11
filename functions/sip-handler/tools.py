def ceph_str_hash_linux(s):
    hash = 0

    for c in s:
        o = ord(c)
        hash = (hash + (o << 4) + (o >> 4)) * 11

    return hash

