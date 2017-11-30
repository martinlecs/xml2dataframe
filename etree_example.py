#import xml.etree.ElementTree as ET
from lxml import etree as ET
import pandas as pd

path='test-files/useragent.xml'

def xml2df(path):
    xml_data = open(path).read()
    root = ET.XML(xml_data) # Creates an element tree that stores the xml info
    all_records = [] # List that we will convert into a dataframe
    for i, child in enumerate(root): # loop through our root tree
        record = {} # Place holder for our record (unit)
        for subchild in child: #iterate through the subchildren of user-agent (root) Eg. ID, String
            record[subchild.tag] = subchild.text # tag and text are methods that belong to root
            all_records.append(record)
    return pd.DataFrame(all_records) #return records as DataFrame

def fast_iter(path):
    all_records = []
    context = ET.iterparse(path, tag='user-agent')
    for event, elem in context:
        all_records.append(xml2df2(elem))
        elem.clear() # Free references to nodes from each iteration
        while elem.getprevious() is not None:
            del elem.getparent()[0] # Eliminate now-empty references from root node to the tag
    del context
    return pd.DataFrame(all_records)

def printElem(elem):
    print(ET.tostring(elem))

def xml2df2(elem):
    record = {}
    for child in elem:
        record[child.tag] = child.text
    return record
