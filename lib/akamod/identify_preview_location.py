#identify_preview_location (akara module - new)
#Responsible for: adding a field to a document with the URL where we should expect to the find the thumbnail
#Usage:  as a module in the enrichment pipeline, as part of the standard ingest process

@simple_service('POST', 'http://purl.org/la/dp/indentify_preview_location', 'identify_preview_location', 'application/json')
def identify_preview_location(body, ctype):
    """
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        ersponse.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    
    URL_FIELD_NAME = u"preview_source_url"
    url = data['source']
    (base_url, rest) = url.split("u?")
    p = rest.split(",")
    thumb_url = "%scgi-bin/thumbnail.exe?CISOROOT=%s&amp;CISOPTR=" % (base_url, p[0], p[1])
    body[URL_FIELD_NAME] = thumb_url
    return json.dumps(data)
