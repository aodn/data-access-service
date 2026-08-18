def compare_dict_keys(orig: dict, other: dict):
    common = set(orig.keys()).intersection(other.keys())
    return bool(common), common
