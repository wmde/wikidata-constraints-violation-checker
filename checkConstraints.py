import requests

OUTPUT_DELIMITER = ';'
STATEMENT_COUNT_URL = 'https://www.wikidata.org/w/api.php?action=query&prop=pageprops&ppprop=wb-claims&format=json'
CONSTRAINT_CHECK_URL = 'https://www.wikidata.org/w/api.php?format=json&action=wbcheckconstraints'
q_id = 'Q64'

r = requests.get(CONSTRAINT_CHECK_URL + '&id=' + q_id).json()

# https://www.wikidata.org/w/api.php?format=json&id=Q78469&action=wbcheckconstraints

warnings = 0
suggestions = 0
bad_paramteres = 0
violations = 0

violated_statements = 0

def printHeader():
    print(OUTPUT_DELIMITER.join(['QID','statements','violations','warnings','suggestions','violated_statements']))

def countStatements(q_id):
    statementCountResponse = requests.get(STATEMENT_COUNT_URL + '&titles=' + q_id).json()
    pages = statementCountResponse['query']['pages']
    firstPageId = next(iter(pages))
    return pages[firstPageId]['pageprops']['wb-claims']

def incrementCounter(status):
    global violations
    global warnings
    global suggestions
    global bad_paramteres

    if status == 'violation':
        violations+=1
    elif status == 'warning':
        warnings+=1
    elif status == 'suggestion':
        suggestions+=1
    elif status == 'bad-parameters':
        bad_paramteres+=1


statementCount = countStatements(q_id)

for (property_id, constraint_checks) in r['wbcheckconstraints'][q_id]['claims'].items():
        for constraint_check in constraint_checks:
            main_results = constraint_check['mainsnak']['results']
            for main_result in main_results:
                # print("property_id, status:", property_id, main_result['status'])
                incrementCounter(main_result['status'])
            if 'qualifiers' in constraint_check.keys():
                qualifier_items = constraint_check['qualifiers'].items()
                for (qualifier_property_id, qualifier_item) in qualifier_items:
                    for qualifier_constraint_check in qualifier_item:
                        qualifier_results = qualifier_constraint_check['results']
                        for qualifier_result in qualifier_results:
                            # print("property_id, qualifier_property_id, status:", property_id, qualifier_property_id, qualifier_result['status'])
                            incrementCounter(qualifier_result['status'])

            if 'references' in constraint_check.keys():
                reference_items = constraint_check['references']
                for reference_item in reference_items:
                    for (snak_property_id, reference_constraint_checks) in reference_item['snaks'].items():
                        for reference_constraint_check in reference_constraint_checks:
                            reference_results = reference_constraint_check['results']
                            for reference_result in reference_results:
                                # print("property_id, snak_property_id, status:", property_id, snak_property_id, reference_result['status'])
                                incrementCounter(reference_result['status'])

printHeader()

# list of str-mapped int values, delimited by OUTPUT_DELIMITER
print(OUTPUT_DELIMITER.join(map(str,[q_id, statementCount, violations, warnings, suggestions, violated_statements])))