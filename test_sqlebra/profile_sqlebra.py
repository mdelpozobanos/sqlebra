import os
from sqlebra.database.sqlite import open_sqlitedatabase
import sqlebra.object.numpy
import cProfile
import numpy as np

# X = (0, 0.1, True, 'profile', None,
#      (1, 2, 3), [4, 5, 6], {'a': 7, 'b': 8, 'c': 9},
#      np.random.randn(4))

X = (tuple(range(10000)), list(range(10000, 20000)), np.random.randn(10000))

# Allocate database
file_name = os.path.join(os.path.dirname(__file__), 'files', 'profile.mem')


def main():
    db = open_sqlitedatabase(file=file_name, name='sqlebra', mode='+', autocommit=False)

    # Do stuff
    for n, v in enumerate(X):
        db['var' + str(n)] = v


if __name__ == '__main__':
    cProfile.run('main()', filename='profile_output.prof')
    # main()

# snakeviz test_sqlebra/profile_output.prof