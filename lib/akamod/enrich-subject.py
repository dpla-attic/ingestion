from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
import re

@simple_service('POST', 'http://purl.org/la/dp/enrich-subject', 'enrich-subject', 'application/json')
def enrichsubject(body,ctype,action="enrich-subject",prop="sourceResource/subject"):
    '''   
    Service that accepts a JSON document and enriches the "subject" field of that document
    by: 

    a) converting converting subjects that are raw strings to dictionaries of the form: { name: <subject> }
    b) applying a set of regexps to do data cleanup
    
    By default works on the 'subject' field, but can be overridden by passing the name of the field to use
    as a parameter
    '''   
    
    TAGS_FOR_STRIPPING = '[\.\' ";]*' # Tags for stripping at beginning and at the end.
    REGEXPS = ('\s*-{2,4}\s*', '--'), \
              ('\s*-\s*-\s*', '--'), \
              ('^' + TAGS_FOR_STRIPPING, ''), \
              (TAGS_FOR_STRIPPING + '$','')

    def cleanup(s):
        try:
            s = s.strip()
        except AttributeError:  # when s is None
            s = ''
        for pattern, replace in REGEXPS:
            s = re.sub(pattern, replace, s)
        if len(s) > 2:
            s = s[0].upper() + s[1:]
        else:
            s = None
        return s

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if exists(data,prop):
        v = getprop(data,prop)
        subject = []
        for s in (v if not isinstance(v,basestring) else [v]):
            subj = cleanup(s)
            if subj:
                subject.append({ "name" : subj })

        setprop(data,prop,subject)

    return json.dumps(data)
