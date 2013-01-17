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

        if isinstance(first, str):
            self.first = json.loads(first)
        else:
            self.first = first

        if isinstance(second, str):
            self.second = json.loads(second)
        else:
            self.second = second

        self._diff = self._generate_diff()
        print self._diff

    def _generate_diff(self):
        """
        Returns:
            Dictionary difference as a dictionary.
        """
        diff = {}
        for k in self.first.keys():
            if not self.second.has_key(k):
                diff[k] = (self.first[k], "KEY NOT FOUND")
            elif self.first[k] != self.second[k]:
                diff[k] = (self.first[k], self.second[k])
        for k in self.second.keys():         
            if not self.first.has_key(k):
                diff[k] = (self.second[k], "KEY NOT FOUND")                
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
            Prints nicely the difference between dictionaries.
        """
        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint( self._diff )
        print "aaaa"
