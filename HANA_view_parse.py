import xml.etree.ElementTree as ET


def parse_xml(xmlfile):
    tree = ET.parse(xmlfile)
    root = tree.getroot()

    print('ROOT:')
    print(root, '-', root.attrib)
    # print(root.attrib['id'])

    print('DATASOURCES:')
    for item in tree.find('dataSources'):
        print(item.attrib['id'], '-', item.tag, '-', item.attrib)
        print('\tChildren:', [child for child in item])

    print('================================')
    print('CALCULATION VIEWS:')
    for item in tree.find('calculationViews'):
        print(item.attrib['id'], '-', item.tag, '-', item.attrib)
        print('\tChildren:', [child for child in item])

    print('================================')
    print('LOGICAL MODELS:')
    for item in tree.findall('logicalModel'):
        print(item.attrib['id'], '-', item.tag, '-', item.attrib)
        print('\tChildren:', [child for child in item])
