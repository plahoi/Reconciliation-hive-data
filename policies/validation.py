# coding=utf-8
''' Data validators
'''

def int_validator(src, dst, tolerance=None):
    ''' Validates integers on equal clause

    Arguments:
        src (int): Source set value
        dst (int): Destination set value
        tolerance (float): Diviation level in range (0-1) where 0.1 equals 10%

    Returns:
        None if ok
        String if not ok

    '''
    are_equal = True

    if not src == dst:
        are_equal = False

    if tolerance:
        try:
            # If diviation is less than allowed then ok
            if abs(src - dst) <= src * tolerance:
                are_equal = True
        except TypeError as e:
            return str(e)

    if not are_equal:
        return 'Values {} and {} are not equal'.format(src, dst)

def string_validator(src, dst):
    if not src == dst:
        return 'Values {} and {} are not equal'.format(src, dst)

def date_validator(src, dst):
    if not src == dst:
        return 'Values {} and {} are not equal'.format(src, dst)
