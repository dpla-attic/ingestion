class DictDiffer():

    def diff(self, first, second):
        diff = {}
        for k in first.keys():
            if not second.has_key(k):
                diff[k] = (first[k], "KEY NOT FOUND")
            elif first[k] != second[k]:
                diff[k] = (first[k], second[k])
        for k in second.keys():         
            if not first.has_key(k):
                diff[k] = (second[k], "KEY NOT FOUND")                
        return diff
            
    def same(self, first, second):
        return self.diff(first, second) == {}
