import asyncio
from aiohttp import ClientSession
import csv
import json

OUTPUT_DELIMITER = ';'
STATEMENT_COUNT_URL = 'https://www.wikidata.org/w/api.php?action=query&prop=pageprops&ppprop=wb-claims&format=json'
CONSTRAINT_CHECK_URL = 'https://www.wikidata.org/w/api.php?format=json&action=wbcheckconstraints'

# TODO
violated_statements = 0

def printHeader():
    print(OUTPUT_DELIMITER.join(['QID','statements','violations','warnings','suggestions','violated_statements']))

def printResults(q_id, statementCount, constraintChecks):
    # list of str-mapped int values, delimited by OUTPUT_DELIMITER
    print(OUTPUT_DELIMITER.join(map(str, [
        q_id,
        statementCount,
        constraintChecks['violations'],
        constraintChecks['warnings'],
        constraintChecks['suggestions'],
        violated_statements]
    )))

async def countStatements(q_id):
    async with ClientSession() as session:
        async with session.get(STATEMENT_COUNT_URL + '&titles=' + q_id) as statementCountResponse:
            statementCountResponse = await statementCountResponse.read()
            r = json.loads(str(statementCountResponse, 'utf-8'))
            pages = r['query']['pages']
            firstPageId = next(iter(pages))
            return pages[firstPageId]['pageprops']['wb-claims']

async def checkConstraints(q_id):
    counter = {
        'violations': 0,
        'warnings': 0,
        'suggestions': 0
    }

    async with ClientSession() as session:
        async with session.get(CONSTRAINT_CHECK_URL + '&id=' + q_id) as r:
            r = await r.read()
            parsed_response = json.loads(str(r, 'utf-8'))

            for (property_id, constraint_checks) in parsed_response['wbcheckconstraints'][q_id]['claims'].items():
                for constraint_check in constraint_checks:
                    main_results = constraint_check['mainsnak']['results']
                    for main_result in main_results:
                        # print("property_id, status:", property_id, main_result['status'])
                        counter = incrementCounter(main_result['status'], counter)
                    if 'qualifiers' in constraint_check.keys():
                        qualifier_items = constraint_check['qualifiers'].items()
                        for (qualifier_property_id, qualifier_item) in qualifier_items:
                            for qualifier_constraint_check in qualifier_item:
                                qualifier_results = qualifier_constraint_check['results']
                                for qualifier_result in qualifier_results:
                                    # print("property_id, qualifier_property_id, status:", property_id, qualifier_property_id, qualifier_result['status'])
                                    counter = incrementCounter(qualifier_result['status'], counter)

                    if 'references' in constraint_check.keys():
                        reference_items = constraint_check['references']
                        for reference_item in reference_items:
                            for (snak_property_id, reference_constraint_checks) in reference_item['snaks'].items():
                                for reference_constraint_check in reference_constraint_checks:
                                    reference_results = reference_constraint_check['results']
                                    for reference_result in reference_results:
                                        # print("property_id, snak_property_id, status:", property_id, snak_property_id, reference_result['status'])
                                        counter = incrementCounter(reference_result['status'], counter)

            return counter


def incrementCounter(status, counter):
    if status == 'violation':
        counter['violations'] += 1
    elif status == 'warning':
        counter['warnings'] += 1
    elif status == 'suggestion':
        counter['suggestions'] += 1

    return counter

async def main():
    printHeader()

    with open('data/input.csv', newline='') as inputFile:
        lines = list(csv.reader(inputFile))

    for fields in lines:
        q_id=fields[0]
        statementCount = await countStatements(q_id)
        constraintChecks = await checkConstraints(q_id)
        printResults(q_id, statementCount, constraintChecks)


loop=asyncio.get_event_loop()
loop.run_until_complete(main())
