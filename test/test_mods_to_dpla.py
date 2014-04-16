import dplaingestion.akamod.mods_to_dpla as mods_to_dpla


def test_nypl_date_multiple_origininfo():
	"""
	NYPL date transform picks last originInfo element
	"""
	original = {
		"originInfo": [
			{
	            "dateCreated": [
	                {
	                    "#text": "1918",
	                    "encoding": "w3cdtf",
	                    "point": "start"
	                },
	                {
	                    "#text": "1948",
	                    "encoding": "w3cdtf",
	                    "point": "end"
	                }
	            ]
			},
			{
	            "dateCaptured": {
	                "#text": "1919",
	                "encoding": "w3cdtf"
	            }
			}
		]
	}
	expected = {
		"date": ["1919"]
	}
	result = mods_to_dpla.date_publisher_and_spatial_transform_nypl(
			original, "originInfo")
	print result
	assert result == expected

def test_nypl_date_one_origininfo_daterange():
	"""
	NYPL date transform works with one originInfo element (date range)
	"""
	original = {
        "originInfo": {
            "dateCreated": [
                {
                    "#text": "1861",
                    "encoding": "w3cdtf",
                    "keyDate": "yes",
                    "point": "start"
                },
                {
                    "#text": "1871",
                    "encoding": "w3cdtf",
                    "point": "end"
                }
            ]
        }
	}
	expected = {
		"date": ["1861 - 1871"]
	}
	result = mods_to_dpla.date_publisher_and_spatial_transform_nypl(
			original, "originInfo")
	print result
	assert result == expected

def test_nypl_date_one_origininfo_singledate():
	"""
	NYPL date transform works with one originInfo element (one date)
	"""
	original = {
        "originInfo": {
            "dateCreated":
                {
                    "#text": "1861",
                    "encoding": "w3cdtf",
                    "keyDate": "yes",
                    "point": "start"
                }
        }
	}
	expected = {
		"date": ["1861"]
	}
	result = mods_to_dpla.date_publisher_and_spatial_transform_nypl(
			original, "originInfo")
	print result
	assert result == expected

