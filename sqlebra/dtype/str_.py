from ..object.single import Single
import builtins


class str_(Single):
    """SQLobject of type str"""

    pyclass = builtins.str
    col_val = 'txt_val'

    def __len__(self):
        return len(self.x)
