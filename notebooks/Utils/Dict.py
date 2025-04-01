from typing import Dict


class DictUtils(object):
    def sort_dict_by_values(self, dct: Dict[str, any]) -> Dict[str, any]:
        sorted_dict = {k: v for k, v in sorted(dct.items(), key=lambda item: item[1])}
        return sorted_dict