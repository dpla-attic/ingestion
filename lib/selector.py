import copy

PATH_DELIM = '/'

def getprop(obj,path,keyErrorAsNone=False):
    """
    Returns the value of the key identified by interpreting
    the path as a delimited hierarchy of keys
    """

    if isinstance(obj, list):
        obj
        
    if '/' not in path:
        if keyErrorAsNone:
            return obj.get(path)
        else:
            return obj[path]

    pp,pn = tuple(path.lstrip(PATH_DELIM).split(PATH_DELIM,1))
    if pp not in obj:
        if not keyErrorAsNone:
            raise KeyError('Path not found in object: %s (%s)'%(path,pp))
        else:
            return None

    if exists(obj, pp):
        return getprop(obj[pp],pn,keyErrorAsNone)
    else:
        if not keyErrorAsNone:
            raise KeyError('Path not found in object: %s (%s)'%(path,pp))
        else:
            return None


def setprop(obj,path,val,keyErrorAsNone=False):
    """
    Sets the value of the key identified by interpreting
    the path as a delimited hierarchy of keys
    """
    if '/' not in path:
        obj[path] = val
        return

    pp,pn = tuple(path.lstrip(PATH_DELIM).split(PATH_DELIM,1))
    if pp not in obj:
        if not keyErrorAsNone:
            raise KeyError('Path not found in object: %s (%s)'%(path,pp))
        else:
            return None

    return setprop(obj[pp],pn,val,keyErrorAsNone)

def delprop(obj,path,keyErrorAsNone=False):
    """
    Removes the key-value from obj
    """
    if '/' not in path:
        obj.pop(path,None)
        return

    pp,pn = tuple(path.lstrip(PATH_DELIM).split(PATH_DELIM,1))
    if pp not in obj:
        if not keyErrorAsNone:
            raise KeyError('Path not found in object: %s (%s)'%(path,pp))
        else:
            return None

    return delprop(obj[pp],pn,keyErrorAsNone=False)

def exists(obj,path):
    """
    Returns True if the key path exists in the object
    """
    found = False
    try:
        found = getprop(obj,path,keyErrorAsNone=False) != None
    except KeyError:
        pass
    except TypeError:
        pass

    return found

TEST_OBJ = {
    'person': {
        'name': 'john smith',
        'age': 25
    },
    'address': {
        'city': 'ottawa',
        'street': '22 acacia'
    },
    'geek-code': 'GE d? s: a+'
}
def test_get():
    assert getprop(TEST_OBJ,PATH_DELIM.join(('person','name'))) == TEST_OBJ['person']['name']
    assert getprop(TEST_OBJ,PATH_DELIM.join(('person','age'))) == TEST_OBJ['person']['age']
    assert getprop(TEST_OBJ,'address') == TEST_OBJ['address']
    assert getprop(TEST_OBJ,'geek-code') == TEST_OBJ['geek-code']
    assert getprop(TEST_OBJ,PATH_DELIM.join(('','person','name'))) == TEST_OBJ['person']['name']

    assert exists(TEST_OBJ,PATH_DELIM.join(('person','name')))
    assert exists(TEST_OBJ,PATH_DELIM.join(('person','age')))
    assert exists(TEST_OBJ,'geek-code')
    assert not exists(TEST_OBJ,'kajsdlj')
    assert not exists(TEST_OBJ,PATH_DELIM.join(('person','sex')))

def test_set_string():
    o = copy.deepcopy(TEST_OBJ)
    setprop(o,PATH_DELIM.join(('address','street')),'22 sussex')
    assert getprop(o,PATH_DELIM.join(('address','street'))) == '22 sussex'

def test_set_dict():
    o = copy.deepcopy(TEST_OBJ)
    setprop(o,'person',{'name': 'jason jones','age': 38})
    assert getprop(o,PATH_DELIM.join(('person','age'))) == 38
