from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists

@simple_service('POST',
                'http://purl.org/la/dp/artstor_spatial_to_dataprovider',
                'artstor_spatial_to_dataprovider', 'application/json')
def artstor_spatial_to_dataprovider(body, ctype,
                                    prop="sourceResource/spatial"):
    """Sets the dataProvider from sourceResource/spatial by:

       1. Deleting the dataProvider field
       2. Splitting on semicolon if sourceResource/spatial is a string
       3. Moving the first sourceResource/spatial value to dataProvider for
          DPLA* collections
       4. Moving the "Repository: " value to dataProvider for SS* collections
       5. Removing the sourceResource/spatial field for DPLA* collections
       6. Removing any "Accession number: " values from sourceResource/spatial
          for SS* collections
       7. Removing the string "Repository: " from the dataProvider value
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    delprop(data, "dataProvider")
    if exists(data, prop):
        v = getprop(data, prop)
        if isinstance(v, basestring):
            v = v.split(";")

        spatial = []
        data_provider = None
        collection = getprop(data, "originalRecord/setSpec", True)
        if collection.startswith("DPLA"):
            data_provider = v[0]
        elif collection.startswith("SS"):
            spatial = []
            for s in v:
                if "Repository" in s:
                    data_provider = s
                elif "Accession" not in s:
                    spatial.append(s)

        delprop(data, prop)
        if spatial:
            setprop(data, prop, spatial)
        if data_provider:
            setprop(data, "dataProvider", data_provider.replace("Repository: ",
                                                                ""))
    return json.dumps(data)
