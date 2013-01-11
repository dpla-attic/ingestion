import sys, os
from amara import bindery

item_f = open(sys.argv[1],'r')
item = bindery.parse(item_f)

hier_items = item.archival_description.hierarchy.hierarchy_item
for hi in hier_items:
    htype = unicode(hi.hierarchy_item_lod).replace(' ','')
    hid = hi.hierarchy_item_id

    if hid:
        hier_fname = os.path.join(os.path.dirname(sys.argv[1]),"%s_%s.xml"%(htype,hid))
        print hier_fname
        hier_f = open(hier_fname,'r')

        hier = bindery.parse(hier_f)
        print "... belongs to "+str(hier.archival_description.title)

        hier_f.close()

item_f.close()
