#
# Usage: python poll_profiles.py <profiles-directory> <data-directory>

import sys, os
import httplib2
import datetime
from amara.thirdparty import json

PROFILE_EXT = '.pjs'
PROFILE_DATA_EXT = '.sjs'
PROFILE_DATA_NAME = lambda d,n: os.path.join(d,'%s%s'%(n.rsplit(PROFILE_EXT,1)[0], PROFILE_DATA_EXT))

def process_profile(indir,outdir,profname):

    fprof = open(os.path.join(indir,profname),'r')
    try:
        profile = json.load(fprof)
    except Exception as e:
        profile = None;

    fprof.close()

    if not profile:
        print >> sys.stderr, 'Error reading source profile.'
        return False

    H = httplib2.Http('/tmp/.pollcache')
    H.force_exception_as_status_code = True
    resp, content = H.request(profile[u'endpoint_URL'])
    if not resp[u'status'].startswith('2'):
        print >> sys.stderr, '  HTTP error (',resp[u'status'],') resolving URL: ',content[:80]
        return False

    # Save retrieved data
    fout = open(PROFILE_DATA_NAME(outdir,profname),'w')
    fout.write(content)
    fout.close()

    # Update profile metadata and save
    profile[u'last_checked'] = datetime.datetime.now().isoformat()
    fprof = open(os.path.join(indir,profname),'w')
    json.dump(profile,fprof)
    fprof.close()

    return True

if __name__ == '__main__':

    # Verify that both given directories exist
    for d in sys.argv[1:]:
        dirExists = False
        try:
            if os.stat(d): dirExists = True
        except:
            pass

        if not dirExists:
            print >> sys.stderr, 'Directory '+d+' does not exist. Aborting.'
            sys.exit(1)

    for profile in filter(lambda x: x.endswith(PROFILE_EXT), os.listdir(sys.argv[1])):
        print >> sys.stderr, 'Processing profile: '+profile
        process_profile(sys.argv[1], sys.argv[2], profile)
