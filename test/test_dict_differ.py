import sys
from dplaingestion.dict_differ import DictDiffer

def test_dict_differ():
    original = {
        "a": "value",
        "b": ["value", "value"],
        "c": {
            "cc": {
                "1": "value"
            }
        },
        "d": "value",
        "e": "value",
        "f": {
            "ff": {
                "1": "value",
                "2": "value",
                "3": "value"
            }
        }
    }
    new = {
        "a": "value",
        "b": ["value"],
        "c": {
            "cc": {
                "1": "changed value",
                "2": "added value"
            },
            "dd": "added",
            "ee": {
                "eee": {
                    "1": "added",
                    "2":  {
                        "eeeee": "added"
                    }
                }
            }
        },
        "d": "changed",
        "f": {
            "ff": {
                "1": "value",
                "3": "changed"
            }
        }
    }
   

    diff = DictDiffer(new, original)

    assert list(set(["e", "f/ff/2"])) == list(set(diff.removed()))
    assert list(set(["b", "c/cc/1", "d", "f/ff/3"])) == list(set(diff.changed()))
    assert list(set(["c/cc/2", "c/dd", "c/ee/eee/1", "c/ee/eee/2/eeeee"])) == list(set(diff.added()))
