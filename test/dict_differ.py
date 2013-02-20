import json


def pinfo(*data):
    """
    Prints all the params in separate lines.
    """
    for d in data:
        print d


def assert_same_jsons(this, that):
    """
    Checks if the dictionaries are the same.
    It compares the keys and values.
    Prints diff if they are not exact and throws exception.
    """
    d = DictDiffer(this, that)

    if not d.same():
        d.print_diff()
        assert this == that


class DictDiffer:
    """
    Class for creating nicely looking dictionary diffs.
    """

    def __init__(self, first, second):

        if isinstance(first, basestring):
            self.first = json.loads(first)
        else:
            self.first = first

        if isinstance(second, basestring):
            self.second = json.loads(second)
        else:
            self.second = second

        self._diff = self._generate_diff()

    def _generate_diff(self):
        """
        Returns:
            Dictionary difference as a dictionary.
        """
        diff = {}
        for k in self.first.keys():
            if not k in self.second:
                diff[k] = ('KEY NOT FOUND IN SECOND DICT', self.first[k])
            elif self.first[k] != self.second[k]:
                diff[k] = {"DIFFERENT VALUES":
                            {'FIRST DICT': self.first[k],
                              'SECOND DICT': self.second[k]}
                            }

        for k in self.second.keys():
            if not k in self.first:
                diff[k] = ("KEY NOT FOUND IN FIRST DICT", self.second[k])
        return diff

    def same(self):
        """
        Returns:
            True if provided dictionaries are the same.
            False otherwise.
        """
        return self._diff == {}

    def diff(self):
        """
        Returns:
            Dictionary difference between the dictionaries.
        """
        return self._diff

    def print_diff(self):
        """
        Returns:
            Nothing

        Side effects:
            Prints nicely the difference between dictionaries.
        """
        import pprint
        pp = pprint.PrettyPrinter(indent=2)
        pp.pprint(self._diff)
