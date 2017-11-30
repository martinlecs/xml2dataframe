import os
import xml.etree.ElementTree as ET
from io import StringIO, BytesIO
import pandas as ps


def getDF(path, tag):
    if not os.path.isfile(path):
        raise ValueError("Invalid file path")

    f = open(path, 'rb')
    xml_data = f.read()
    f.close()
    
    return xml2dataframe(xml_data.decode('utf-8'), tag)

def getRoot(data):
    #strips namespaces from the xml
    it = ET.iterparse(StringIO(data))
    for _, el in it:
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
    return it.root

def xml2dataframe(data,tag,down_tags=[],display_dupes=False):
    #Based on code from:
    #  http://www.austintaylor.io/lxml/python/pandas/xml/dataframe/2016/07/08/convert-xml-to-pandas-dataframe/
    def createElem(curr,par_tag=None):
        children = list(curr)
        children = [x for x in children if x.tag not in down_tags]
        if par_tag == None:
            tag = ""
        elif par_tag == "":
            tag = curr.tag
        else:
            tag = par_tag + "-" + curr.tag

        #adds any key values
        if len(curr.keys()) > 0:
            for key in curr.keys():
                temp_key = key
                if tag != "":
                    temp_key = tag + "-" + key
                if temp_key in element.keys() and display_dupes == True:
                    already_keys = [x for x in element.keys() if re.match('^%s_?\d*$' % temp_key ,x)]
                    temp_key = temp_key + '_' + str(len(already_keys) - 1)

                element[temp_key] = curr.get(key)

        if len(children) == 0:
            temp_key = tag
            if temp_key in element.keys() and display_dupes == True:
                already_keys = [x for x in element.keys() if re.match('^%s_?\d*$' % temp_key ,x)]
                temp_key = temp_key + '_' + str(len(already_keys) - 1)

            element[temp_key] = curr.text
        else:
            [createElem(x,tag) for x in children]


    def expandDown(elem,next_tags):
        #elem = {'elem': {...}, 'root': root_element}
        if len(next_tags) <= 0:
            return [elem['elem']]

        out_elems = []
        for temp_child in elem['root'].findall('.//%s' % next_tags[0]):
            nonlocal element
            element = {}
            createElem(temp_child)
            temp_elem = dict((next_tags[0] + '-' + key,val) for key,val in element.items())
            #appends a concat of the old and new elems
            out_elems.append({'elem':{**elem['elem'],**temp_elem}, 'root':temp_child}) 

        out = []
        for res in [expandDown(x,next_tags[1:]) for x in out_elems]:
            out += res
        return out

    root = getRoot(data) # loading this into whole memory
    all_records = []
    headers = []


    for child in root.findall('.//%s' % tag):
        if child.tag == tag and len(list(child)) > 0:
            element = {}
            createElem(child)

            all_elems = [element]

            if len(down_tags) > 0:
                all_elems = expandDown({'elem':element,'root':child},down_tags)
                #print(all_elems)

            for element in all_elems:
                record = []
                for field, val in element.items():
                    if field not in headers:
                        headers.append(field)
                for field in headers:
                    if field in element.keys():
                        val = element.get(field)
                        if val is not None:
                            val = val.replace("~", ">")
                        record.append(val)
                    else:
                        record.append("")

                all_records.append(record)
                element.clear()
                while element.getprevious() is not None:
                    del element.getparent()[0]
    del root
    return ps.DataFrame(all_records, columns=headers)
