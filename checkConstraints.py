#!/usr/bin/python3

import sys
import os
import getopt
import asyncio
from aiohttp import ClientSession
import csv
import json
from datetime import datetime
import random

OUTPUT_DELIMITER = ';'
STATEMENT_COUNT_URL = 'https://www.wikidata.org/w/api.php?action=query&prop=pageprops&ppprop=wb-claims&format=json'
CONSTRAINT_CHECK_URL = 'https://www.wikidata.org/w/api.php?format=json&action=wbcheckconstraints'

# TODO
violated_statements = 0

def printHeader(outputFileName):
    with open(outputFileName, 'w') as outputFile:
        print(OUTPUT_DELIMITER.join([
            'QID',
            'statements',
            'violations_mandatory_level',
            'violations_normal_level',
            'violations_suggestion_level',
            'violated_statements'
        ]), file=outputFile)

def printResults(q_id, statementCount, constraintChecks, outputFileName):
    with open(outputFileName, 'a') as outputFile:
        # list of str-mapped int values, delimited by OUTPUT_DELIMITER
        print(OUTPUT_DELIMITER.join(map(str, [
            q_id,
            statementCount,
            constraintChecks['violations'],
            constraintChecks['warnings'],
            constraintChecks['suggestions'],
            constraintChecks['violated_statements']
            ]
        )), file=outputFile)

async def countStatements(q_id):
    # Returns the number of statements on the given entity, returns False if the
    # entity does not exist or is a redirect.
    async with ClientSession() as session:
        async with session.get(STATEMENT_COUNT_URL + '&titles=' + q_id) as statementCountResponse:
            statementCountResponse = await statementCountResponse.read()
            r = json.loads(str(statementCountResponse, 'utf-8'))
            pages = r['query']['pages']
            firstPageId = next(iter(pages))
            try:
                statementCount = pages[firstPageId]['pageprops']['wb-claims']
            except KeyError:
                return False
            return statementCount

async def checkConstraints(q_id):
    counter = {
        'violations': 0,
        'warnings': 0,
        'suggestions': 0,
        'violated_statements': 0,
        'statement_is_violated': False
    }

    async with ClientSession() as session:
        try:
            async with session.get(CONSTRAINT_CHECK_URL + '&id=' + q_id) as r:
                if r.status != 200:
                    return False
                r = await r.read()
                parsed_response = json.loads(str(r, 'utf-8'))
                if('error' in parsed_response):
                    return False
                claims = parsed_response['wbcheckconstraints'][q_id]['claims']
                # claims is a list (not a dict) if it's empty... yikes.
                if not type(claims) is dict:
                    return counter

                for (property_id, statement_group) in claims.items():
                    for statement in statement_group:
                        counter['statement_is_violated'] = False

                        violated_mainsnaks = statement['mainsnak']['results']
                        for violated_mainsnak in violated_mainsnaks:
                            # print("property_id, status:", property_id, main_result['status'])
                            counter = incrementCounter(violated_mainsnak['status'], counter)

                        if 'qualifiers' in statement.keys():
                            qualifier_items = statement['qualifiers'].items()
                            for (qualifier_property_id, qualifier_item) in qualifier_items:
                                for qualifier_constraint_check in qualifier_item:
                                    qualifier_results = qualifier_constraint_check['results']
                                    for qualifier_result in qualifier_results:
                                        # print("property_id, qualifier_property_id, status:", property_id, qualifier_property_id, qualifier_result['status'])
                                        counter = incrementCounter(qualifier_result['status'], counter)

                        if 'references' in statement.keys():
                            reference_items = statement['references']
                            for reference_item in reference_items:
                                for (snak_property_id, reference_constraint_checks) in reference_item['snaks'].items():
                                    for reference_constraint_check in reference_constraint_checks:
                                        reference_results = reference_constraint_check['results']
                                        for reference_result in reference_results:
                                            # print("property_id, snak_property_id, status:", property_id, snak_property_id, reference_result['status'])
                                            counter = incrementCounter(reference_result['status'], counter)
                return counter
        except asyncio.exceptions.TimeoutError:
            return False


def incrementCounter(status, counter):
    # ignore
    if status == 'bad-parameters':
        return counter

    if status == 'violation':
        counter['violations'] += 1
    elif status == 'warning':
        counter['warnings'] += 1
    elif status == 'suggestion':
        counter['suggestions'] += 1

    if counter['statement_is_violated'] == False:
        counter['statement_is_violated'] = True
        counter['violated_statements'] += 1

    return counter

async def main(argv):
    inputFileName = ''
    outputFileName = ''
    numberOfItems = 0

    try:
        opts, args = getopt.getopt(argv,"hi:o:r:",["help","ifile=","ofile=","random="])
    except getopt.GetoptError:
        print('checkConstraints.py -i <inputfile> | -r <number of items> [-o <outputfile>]')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print('checkConstraints.py -i <inputfile> | -r <number of items> [-o <outputfile>]')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputFileName = arg
        elif opt in ("-o", "--ofile"):
            outputFileName = arg
        elif opt in ("-r", "--random"):
            numberOfItems = arg

    #TODO: can be simplified with xor logic
    if(not (inputFileName or numberOfItems) or (inputFileName and numberOfItems)):
        print('checkConstraints.py -i <inputfile> | -r <number of items> [-o <outputfile>]')
        sys.exit(2)

    if (not inputFileName and not outputFileName):
        outputFileName = "data/random-" + datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + ".out.csv"
    if(not outputFileName):
        name, extension = os.path.splitext(inputFileName)
        outputFileName = name + ".out" + extension

    printHeader(outputFileName)

    if numberOfItems == 0:
        with open(inputFileName, newline='') as inputFile:
            lines = list(csv.reader(inputFile))

        for index, fields in enumerate(lines):
            if((index+1) % 10 == 1):
                print('|', end='', flush=True)
                if((index+1) % 100 == 0):
                    print('',index+1)

            q_id=fields[0]
            print('.', end='', flush=True)
            statementCount = await countStatements(q_id)
            print('\b-', end='', flush=True)
            if statementCount is False:
                print('\bx', end='', flush=True)
                continue
            constraintChecks = await checkConstraints(q_id)
            if constraintChecks is False:
                print('\bX', end='', flush=True)
                continue
            print('\b+', end='', flush=True)
            printResults(q_id, statementCount, constraintChecks, outputFileName)

    else:
        index = 1
        numberOfItems = int(numberOfItems)

        while index <= numberOfItems:
            q_id= "Q" + str(random.randint(1, 100000000))
            print('.', end='', flush=True)
            statementCount = await countStatements(q_id)
            print('\b-', end='', flush=True)
            if statementCount is False:
                print('\bx', end='', flush=True)
                continue
            constraintChecks = await checkConstraints(q_id)
            print('\b+', end='', flush=True)
            printResults(q_id, statementCount, constraintChecks, outputFileName)

            if((index+1) % 10 == 0):
                print('|', end='', flush=True)
                if((index+1) % 100 == 0):
                    print('',index+1)

            index+=1

    print()

loop=asyncio.get_event_loop()
loop.run_until_complete(main(sys.argv[1:]))
