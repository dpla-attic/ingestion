import dplaingestion.akamod.marc_to_dpla as marc_to_dpla

def test_get_subject_values_strips_comma():
    """_get_subject_values() strips trailing commas"""
    original = {
                   "ind1": "1",
                   "ind2": "0",
                   "subfield": [
                       {
                           "#text": "Lincoln, Abraham,",
                           "code": "a"
                       },
                       {
                           "#text": "1809-1865",
                           "code": "d"
                       }
                   ],
                   "tag": "600"
               }
    expected = ['Lincoln, Abraham, 1809-1865']
    result = marc_to_dpla._get_subject_values(original, "600")
    print result
    assert result == expected
    

