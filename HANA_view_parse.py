import xml.etree.ElementTree as ET
import re

# TODO: Execute calculation:scenario.attrib.keys() to get the actual key names
"""
*.calculationview XML structure:

calculation:scenario (root)
    {calculation:scenario}.attrib['xmlns:xsi']
    {calculation:scenario}.attrib['xmlns:Calculation']
    {calculation:scenario}.attrib['schemaVersion']
    {calculation:scenario}.attrib['id']
    {calculation:scenario}.attrib['applyPrivilegeType']
    {calculation:scenario}.attrib['checkAnalyticPrivileges']
    {calculation:scenario}.attrib['defaultClient']
    {calculation:scenario}.attrib['defaultLanguage']
    {calculation:scenario}.attrib['visibility']
    {calculation:scenario}.attrib['calculationScenarioType']
    {calculation:scenario}.attrib['dataCategory']
    {calculation:scenario}.attrib['enforceSqlExecution']
    {calculation:scenario}.attrib['executionSemantic']
    {calculation:scenario}.attrib['outputViewType']
    origin
    descriptions
        {descriptions}.attrib['defaultDescription']
    metadata
        {metadata}.attrib['changedAt']
    localvariables
    variablemappings
    datasources
        ...
    calculationviews
        calculationview
            {calculationview}.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"]
            {calculationview}.attrib["id"]
            {calculationview}.attrib['cardinality'] --optional, only for {calculationview}.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"] == 'Calculation:JoinView'
            {calculationview}.attrib['joinType'] --optional, only for {calculationview}.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"] == 'Calculation:JoinView'
            viewattributes
                viewattribute
                    {viewattribute}.attrib['id']
            calculatedviewattributes
                calculatedviewattribute
                    {calculatedviewattribute}.attrib['datatype']
                    {calculatedviewattribute}.attrib['id']
                    {calculatedviewattribute}.attrib['length']
                    formula --optional
            input
                {input}.attrib['node']
                mapping
                    {mapping}.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"]
                    {mapping}.attrib['target']
                    {mapping}.attrib['source']
            joinattribute --optional, only for Calculation:JoinView
                {joinattribute}.attrib['name']
    logicalmodel
        ...
"""
def get_root_obj(xmlfile):
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    return tree, root


def get_calculationView_objs(xml_tree):
    """
    Get all objects in the XML tree tagged with 'calculationViews'.
    :param xml_tree:
    :return:
    """
    # TODO: CalculationView types: (c.attrib['xsi:type']) = ['Calculation:ProjectionView', 'Calculation:JoinView', 'Calculation:AggregationView', 'Calculation:UnionView']
    calc_view_type_dict = {}
    calc_view_objs = xml_tree.find('calculationViews')
    calc_view_obj_ids = [c.attrib['id'] for c in calc_view_objs] # TODO: Remove hashtag from beginning of obj_id?

    for c in calc_view_objs:
        calc_view_type_dict[c.attrib['id']] = c.attrib["{http://www.w3.org/2001/XMLSchema-instance}type"] # TODO: Will this key name remain constant in the future?
        # TODO: Get this string from {calculation:scenario}.attrib['xmlns:xsi']

    return calc_view_objs, calc_view_obj_ids, calc_view_type_dict


def get_input_nodes(calc_view_obj):
    """
    Get all objects (input nodes -- AKA table names) tagged with 'input' in the passed calculationView object.
    :param calc_view_obj:
    :return:
    """
    node_objs = calc_view_obj.findall('input')
    node_obj_ids = [n.attrib['node'] for n in node_objs]
    return node_objs, node_obj_ids

# ======================================================================================================================
def get_filters(calc_view_obj):
    filters = calc_view_obj.findall('filter')
    if len(filters) == 0:
        return None
    else:
        return filters


def parse_filter(filter_obj):
    return filter_obj.text

# ======================================================================================================================
def get_joins(calc_view_objs):
    join_objs = []
    for c in calc_view_objs:
        if 'joinType' in c.attrib.keys():
            # print(f"FOUND A JOIN: {c.attrib['id']} -- {c.attrib['joinType']}")
            # print(c.findall('joinAttribute'))
            join_objs.append(c)
    return join_objs


def parse_aliased_key(raw_key):
    left_table_keyname = re.search("(?:\$)(\w*)(?:\$)", raw_key)[1]
    right_table_keyname = re.search("(?:\$)(\w*)$", raw_key)[1]
    return left_table_keyname, right_table_keyname


def parse_join(join_calc_view_obj):
    nodes, node_ids = get_input_nodes(join_calc_view_obj)
    left_table = node_ids[0]
    right_table = node_ids[1]

    join_type = join_calc_view_obj.attrib['joinType']
    join_keys = [ja.attrib['name'] for ja in join_calc_view_obj.findall('joinAttribute')]
    # print('join_type:', join_type)
    # print('join_keys:', join_keys)

    join_key_phrases = []
    for k in join_keys:
        if '$' in k:
            left_table_keyname, right_table_keyname = parse_aliased_key(k)
        else:
            left_table_keyname = k
            right_table_keyname = k
        left_table_keyname = f"{left_table}.{left_table_keyname}"
        right_table_keyname = f"{right_table}.{right_table_keyname}"

        phrase = f"{left_table_keyname} = {right_table_keyname}"

        join_key_phrases.append(phrase)

    join_type_map = {
        # SAP S4HANA syntax: MS SQL syntax
        "leftOuter": "LEFT OUTER JOIN",
        "rightOuter": "RIGHT OUTER JOIN",
        "inner": "INNER JOIN"
    }
    # https://docs.microsoft.com/en-us/sql/relational-databases/performance/joins?view=sql-server-ver15
    join_str = f"""{node_ids[0]} {join_type_map[join_type]} {node_ids[1]} on {' AND '.join(map(str, join_key_phrases))}"""

    return join_str


def get_formulas(calc_view_obj):
    """
    expressionLanguage="COLUMN_ENGINE" is HANA syntax
    expressionLanguage="SQL" is SQL syntax, can be used directly (replace &quot; with ' -- or delete/ignore entirely)

    :param calc_view_obj:
    :return:
    """
    formula_dict = {}
    calcviewattribs = calc_view_obj.findall('calculatedViewAttributes')
    for c in calcviewattribs:
        attrib_objs = c.findall('calculatedViewAttribute')
        if len(attrib_objs) == 0:
            pass
        else:
            # print(attrib_objs)
            for a in attrib_objs:
                a_id = a.attrib['id']
                expr_lang = a.attrib['expressionLanguage']
                formula_obj = a.find('formula') # Assume only 1 formula per calculationViewAttribute object?
                formula_text = formula_obj.text

                formula_dict["calculatedViewAttribute_id"] = a_id
                formula_dict["formula"] = formula_text

    return formula_dict

# ======================================================================================================================
def get_parse_node_mappings(node_obj):
    """
    Get all mappings (fields) tagged with 'mapping' in the passed input node object.

    'source' refers to the original field name. 'target' refers to the field name as it is referred later.
    Source and target will be identical unless the view assigns an alias name.
    :param node_obj:
    :return:
    """
    mappings = node_obj.findall('mapping')
    mappings_str_list = []
    for m in mappings:

        # 'source' NOT in m.attrib.keys() for mappings with x.attrib['xsi:type'] = "Calculation:ConstantAttributeMapping"
        if 'source' in m.attrib.keys():
            mapping_source = m.attrib['source']
        else:
            mapping_source = None

        mapping_target = m.attrib['target']

        if mapping_source != mapping_target:
            mapping_str = f"{mapping_source} as {mapping_target}" # TODO: Fix instances of None source; currently prints as "None as PAYM_CART_STATUS"
        else:
            mapping_str = f"{mapping_source}"

        mappings_str_list.append(mapping_str)

    return mappings_str_list


def parse_calc_views(calc_view_obj):
    node_mappings_dict = {}
    node_objs, node_obj_ids = get_input_nodes(calc_view_obj)
    # print('node_obj_ids:', node_obj_ids)
    for n in node_objs:
        mappings_str_list = get_parse_node_mappings(n)
        node_mappings_dict[n.attrib['node']] = mappings_str_list

    return node_mappings_dict


def compile_node_mappings_string(node_mappings_dict):
    node_select_dict = {}
    for node, maps in node_mappings_dict.items():
        select_str = f"""select {', '.join(map(str, maps))} from {node}"""

        node_select_dict[node] = select_str

    return node_select_dict


def compile_calc_view_string(calc_view_name, node_select_dict):
    calc_view_select_dict = {}
    # all_node_selects = [',\n'.join(node_select) for node, node_select in node_select_dict.items()]
    all_node_selects = [node_select for node, node_select in node_select_dict.items()] # TODO: Fix formatting

    select_str = f"""with {calc_view_name} as (
    {all_node_selects}
    )
    """

    calc_view_select_dict[calc_view_name] = select_str

    return calc_view_select_dict


def full_tree_parse(xmlfile):
    tree, root = get_root_obj(xmlfile)
    calc_view_objs, calc_view_obj_ids, calc_view_type_dict = get_calculationView_objs(tree)
    # TODO: Execute different logic based on ['Calculation:ProjectionView', 'Calculation:JoinView', 'Calculation:AggregationView', 'Calculation:UnionView']

    print('--- JOINS ---')
    join_objs_list = get_joins(calc_view_objs) # TODO: Reformat to search within 1 calc view at a time?
    join_txt_dict = {}
    for j in join_objs_list:
        join_str = parse_join(j)
        join_txt_dict[j.attrib['id']] = join_str
        print(f"{j.attrib['id']}: {join_str}")

    for c in calc_view_objs:
        calc_view_obj_id = c.attrib['id']
        print(f"\n===== CALCULATION VIEW: {calc_view_obj_id} =====")
        print(f"Calculation Type: {calc_view_type_dict[calc_view_obj_id]}")
        print('--- FORMULAS ---')
        formula_dict = get_formulas(c)
        if formula_dict:
            print(formula_dict)

        print('--- FILTERS ---')
        filters = get_filters(c)
        filters_text = []
        if filters:
            for f in filters:
                filters_text.append(parse_filter(f))
        if len(filters_text) > 0:
            # print(["\n".join(f) for f in filters_text])
            print(filters_text)

        print('--- NODES ---')
        node_mappings_dict = parse_calc_views(c)
        print(node_mappings_dict)
        node_select_dict = compile_node_mappings_string(node_mappings_dict)
        # print(node_select_dict)
        calc_view_select_dict = compile_calc_view_string(calc_view_obj_id, node_select_dict)
        print(calc_view_select_dict)

    # print('================================')
    # print('LOGICAL MODELS:')
    # for item in tree.findall('logicalModel'):
    #     print(item.attrib['id'], '-', item.tag, '-', item.attrib)
    #     print('\tChildren:', [child for child in item])
