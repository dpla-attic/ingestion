from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
import re

@simple_service('POST', 'http://purl.org/la/dp/enrich-subject', 'enrich-subject', 'application/json')
def enrichsubject(body,ctype,action="enrich-subject",prop="subject"):
    '''   
    Service that accepts a JSON document and enriches the "subject" field of that document
    by: 

    a) converting converting subjects that are raw strings to dictionaries of the form: { name: <subject> }
    b) applying a set of regexps to do data cleanup
    
    By default works on the 'subject' field, but can be overridden by passing the name of the field to use
    as a parameter
    '''   
    
    REGEXPS = (' -- ','--'), ('\.$',''), ('^\. *','')

    def cleanup(s):
        s = s.strip()
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

    if prop in data:
        subject = []
        for s in (data[prop] if not isinstance(data[prop],basestring) else [data[prop]]):
            subj = cleanup(s)
            if subj:
                subject.append({"name" : subj })

        if subject:
            data[prop] = subject
        else:
            del data[prop]

    return json.dumps(data)
