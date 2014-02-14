from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from dplaingestion.utilities import iterify

DEFAULT_IA_RIGHTS = u"Access to the Internet Archive's Collections is granted for scholarship and research purposes only. Some of the content available through the Archive may be governed by local, national, and/or international laws and regulations, and your use of such content is solely at your own risk."

@simple_service('POST', 'http://purl.org/la/dp/ia-set-rights',
                'ia-set-rights', 'application/json')
def ia_set_rights(body, ctype, prop="sourceResource/rights"):
    """
    Set a default rights statement for IA records missing metadata/possible-copyright-status
    """

    try:
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if not exists(data, "sourceResource/rights"):
        setprop(data, "sourceResource/rights", DEFAULT_IA_RIGHTS)
        if exists(data, "hasView"):
            setprop(data, "hasView/rights", DEFAULT_IA_RIGHTS)

    return json.dumps(data)
