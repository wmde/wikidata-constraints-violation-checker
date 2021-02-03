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
import numpy

CHUNK_SIZE = 50
OUTPUT_DELIMITER = ';'
STATEMENT_COUNT_URL = 'https://www.wikidata.org/w/api.php?action=query&prop=pageprops&ppprop=wb-claims&format=json'
CONSTRAINT_CHECK_URL = 'https://www.wikidata.org/w/api.php?format=json&action=wbcheckconstraints'

EMPTY_RESULTS = {
    'statements': -1,
    'violations_mandatory': -1,
    'violations_normal': -1,
    'violations_suggestion': -1,
    'violated_statements': -1,
    'total_sitelinks': -1,
    'wikipedia_sitelinks': -1,
    'ores_score': -1
}

def parseArguments(argv):
    numberOfItems = False
    outputFileName = ''
    inputFileName = ''

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

    if(not (inputFileName or numberOfItems) or (inputFileName and numberOfItems)):
        print('checkConstraints.py -i <inputfile> | -r <number of items> [-o <outputfile>]')
        sys.exit(2)

    if (not inputFileName and not outputFileName):
        outputFileName = "./random-" + datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + ".out.csv"
    if(not outputFileName):
        name, extension = os.path.splitext(inputFileName)
        outputFileName = name + ".out" + extension

    return numberOfItems, outputFileName, inputFileName

def generateRandomItemIds(numberofItems):
    items = []
    for i in range(numberofItems):
        randomId = 'Q' + str(random.randint(1, 100000000))
        items.append(randomId)

    return items

# generator to create batches of random Q-IDs,
# then fetch the number of statements for them from the query API
async def queryRandomItems(numberOfItems):
    counter = 0
    while counter < numberOfItems:
        batchOfIds = generateRandomItemIds(min(CHUNK_SIZE, numberOfItems - counter))
        batchOfResults = await fetchNumberOfStatements(batchOfIds)
        counter += len(batchOfResults)
        yield batchOfResults

# generator to read batches of Q-IDs from a file,
# then fetch the number of statements for them from the query API
async def queryItemsFromFile(inputFileName):
    with open(inputFileName, newline='') as inputFile:
        lines = [row[0] for row in csv.reader(inputFile)]

    numberOfBatches = (len(lines) // CHUNK_SIZE) + 1
    batches = numpy.array_split(lines, numberOfBatches)
    for batchOfIds in batches:
        batchOfResults = await fetchNumberOfStatements(batchOfIds)
        yield batchOfResults

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

def printResults(itemId, itemResults, outputFileName):
    with open(outputFileName, 'a') as outputFile:
        # list of str-mapped values, delimited by OUTPUT_DELIMITER
        print(OUTPUT_DELIMITER.join(map(str, [
            itemId,
            itemResults['statements'],
            itemResults['violations_mandatory'],
            itemResults['violations_normal'],
            itemResults['violations_suggestion'],
            itemResults['violated_statements']
            ]
        )), file=outputFile)

def logException(exception):
    with open('error.log', 'a') as outputFile:
        print(exception, file=outputFile)

def logErrorMessage(message):
    with open('error.log', 'a') as outputFile:
        print(message, file=outputFile)

def displayProgress(step, overwrite=True):
    character = ''
    if(step < 0):
        #  ANSI escape sequence for 'ERROR' (red)
        character = '\033[91m'
        step *= -1

    if(step == 0):
        character += '.'
    elif(step == 1):
        character += '-'
    elif(step == 2):
        character += '+'
    elif(step == 99):
        character += '|'

    if(overwrite):
        character = '\b' + character

    # turn off color
    character += '\033[0m'

    print(character, end='', flush=True)

async def fetchNumberOfStatements(itemIds):
    # Returns the number of statements on the given entity, returns False if the
    # entity does not exist or is a redirect.
    batchOfResults = []
    async with ClientSession() as session:
        async with session.get(STATEMENT_COUNT_URL + '&titles=' + '|'.join(itemIds)) as statementCountResponse:
            statementCountResponse = await statementCountResponse.read()
            r = json.loads(str(statementCountResponse, 'utf-8'))

    for page in r['query']['pages'].values():
        if not 'pageprops' in page:
            logErrorMessage("Item " + page['title'] + ' does not exist or is a redirect.')
            continue

        results = EMPTY_RESULTS.copy()
        results['itemId'] = page['title']
        results['statements'] = page['pageprops']['wb-claims']
        batchOfResults.append(results)

    return batchOfResults

async def checkConstraints(itemId):
    counter = {
        'violations': 0,
        'warnings': 0,
        'suggestions': 0,
        'violated_statements': 0,
        'statement_is_violated': False
    }

    async with ClientSession() as session:
        async with session.get(CONSTRAINT_CHECK_URL + '&id=' + itemId) as r:
            if r.status != 200:
                raise Exception(
                    'wbcheckconstraint API returned status code ' +
                    str(r.status) +
                    ' for item ' +
                    str(itemId)
                )

            r = await r.read()
            parsed_response = json.loads(str(r, 'utf-8'))
            if 'error' in parsed_response:
                raise Exception(
                    'wbcheckconstraint API returned error \'' +
                    parsed_response['error']['code'] +
                    '\' for item ' + str(itemId)
                )

            claims = parsed_response['wbcheckconstraints'][itemId]['claims']

            # claims is a list (not a dict) if it's empty... yikes.
            if not type(claims) is dict:
                # no statements -> no violations
                return counter

            for (property_id, statement_group) in claims.items():
                for statement in statement_group:
                    counter['statement_is_violated'] = False

                    violated_mainsnaks = statement['mainsnak']['results']
                    for violated_mainsnak in violated_mainsnaks:
                        counter = incrementCounter(violated_mainsnak['status'], counter)

                    if 'qualifiers' in statement.keys():
                        qualifier_items = statement['qualifiers'].items()
                        for (qualifier_property_id, qualifier_item) in qualifier_items:
                            for qualifier_constraint_check in qualifier_item:
                                qualifier_results = qualifier_constraint_check['results']
                                for qualifier_result in qualifier_results:
                                    counter = incrementCounter(qualifier_result['status'], counter)

                    if 'references' in statement.keys():
                        reference_items = statement['references']
                        for reference_item in reference_items:
                            for (snak_property_id, reference_constraint_checks) in reference_item['snaks'].items():
                                for reference_constraint_check in reference_constraint_checks:
                                    reference_results = reference_constraint_check['results']
                                    for reference_result in reference_results:
                                        counter = incrementCounter(reference_result['status'], counter)
            return counter

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

async def checkConstraintViolations(itemId, results):
    displayProgress(0, False)
    try:
        constraintViolations = await checkConstraints(itemId)
        results['violations_mandatory'] = constraintViolations['violations']
        results['violations_normal'] = constraintViolations['warnings']
        results['violations_suggestion'] = constraintViolations['suggestions']
        results['violated_statements'] = constraintViolations['violated_statements']
        displayProgress(2)
    except Exception as ex:
        displayProgress(-2)
        raise Exception(ex)

    return results

async def checkQuality(batchOfItems, outputFileName):
    index = 0
    for index, item in enumerate(batchOfItems):
        itemId = item['itemId']
        try:
            itemResults = await checkConstraintViolations(itemId, item)
            printResults(itemId, itemResults, outputFileName)

            if((index+1) % 10 == 0):
                displayProgress(99, False)

        except Exception as ex:
            logException(ex)
            continue

    return index+1

async def main(argv):
    numberOfItems, outputFileName, inputFileName= parseArguments(argv)

    printHeader(outputFileName)

    if(numberOfItems):
        # we use randomly generated Q-IDs
        batchesOfItems = queryRandomItems(int(numberOfItems))
    else:
        # we read the Q-IDs from a file
        batchesOfItems = queryItemsFromFile(inputFileName)

    totalItemsChecked = 0
    async for batch in batchesOfItems:
        itemsChecked = await checkQuality(batch, outputFileName)
        totalItemsChecked += itemsChecked
        print('', totalItemsChecked)

    print()

loop=asyncio.get_event_loop()
loop.run_until_complete(main(sys.argv[1:]))
