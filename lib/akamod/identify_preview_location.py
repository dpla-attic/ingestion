from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/indentify_preview_location', 'identify_preview_location', 'application/json')
def identify_preview_location(body, ctype):
    """
    Responsible for: adding a field to a document with the URL where we should 
    expect to the find the thumbnail
    """

    try:
        data = json.loads(body)
        url = data['source']
        URL_FIELD_NAME = u"preview_source_url"
        (base_url, rest) = url.split("u?")
        if base_url == "" or rest == "":
            raise Exception("Bad URL.")

        p = rest.split(",")
        if len(p) != 2:
            raise Exception("Bad URL.")

        thumb_url = "%scgi-bin/thumbnail.exe?CISOROOT=%s&amp;CISOPTR=%s" % (base_url, p[0], p[1])
        data[URL_FIELD_NAME] = thumb_url
        return json.dumps(data)
        
    except Exception as e:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return e.message





