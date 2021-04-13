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

OUTPUT_DELIMITER = ';'
STATEMENT_COUNT_URL = 'https://www.wikidata.org/w/api.php?format=json&action=query&prop=pageprops|revisions&ppprop=wb-claims&rvprop=ids'
SITELINK_COUNT_URL = 'https://www.wikidata.org/w/api.php?format=json&action=wbgetentities&props=sitelinks'
CONSTRAINT_CHECK_URL = 'https://www.wikidata.org/w/api.php?format=json&action=wbcheckconstraints'
ORES_URL = 'https://ores.wikimedia.org/v3/scores/wikidatawiki?revids='

# The ORES score is calculated by weight of the most relevant score, see ORES on https://www.wikidata.org/wiki/Wikidata:Item_quality#ORES
ORES_WEIGHTS = {
    "E": 1,
    "D": 2,
    "C": 3,
    "B": 4,
    "A": 5
}

batchSize = 10

def usage(exitCode = False):
    print('checkDataQuality.py -i <inputfile> | -r <number of items> [-o <outputfile> -b <batch-size>]')
    if(exitCode):
        sys.exit(exitCode)


def parseArguments(argv):
    global batchSize
    numberOfRandomItems = False
    outputFileName = ''
    inputFileName = ''

    startMessage = ''

    try:
        opts, args = getopt.getopt(argv,"hi:o:r:b:",["help","ifile=","ofile=","random=","batch-size="])
    except getopt.GetoptError:
        usage(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage(0)
        elif opt in ("-i", "--ifile"):
            inputFileName = arg
        elif opt in ("-o", "--ofile"):
            outputFileName = arg
            startMessage += ', write to ' + outputFileName
        elif opt in ("-r", "--random"):
            numberOfRandomItems = arg
        elif opt in ("-b", "--batch-size"):
            batchSize = int(arg)

    if(not (inputFileName or numberOfRandomItems) or (inputFileName and numberOfRandomItems)):
        usage(2)

    if (not inputFileName and not outputFileName):
        outputFileName = "random-" + datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + ".out.csv"
        startMessage = 'checking quality on ' + str(numberOfRandomItems) + ' random items' +\
                       ', write to ' + outputFileName
    if(not outputFileName):
        name, extension = os.path.splitext(inputFileName)
        outputFileName = name + ".out" + extension
        startMessage = 'checking quality on items from input file ' + inputFileName +\
                       ', write to ' + outputFileName

    startMessage += ', processing in batches of ' + str(batchSize)

    if not os.path.exists('output'):
        os.makedirs('output')

    outputFileName = "./output/" + outputFileName

    print(startMessage)
    return numberOfRandomItems, outputFileName, inputFileName

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
        batchOfIds = generateRandomItemIds(min(batchSize, numberOfItems - counter))
        batchOfResults = await fetchNumberOfStatements(batchOfIds)
        counter += len(batchOfResults)
        yield batchOfResults

# generator to read batches of Q-IDs from a file,
# then fetch the number of statements for them from the query API
async def queryItemsFromFile(inputFileName):
    with open(inputFileName, newline='') as inputFile:
        lines = [row[0] for row in csv.reader(inputFile)]

    numberOfBatches = (len(lines) // batchSize) + 1
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
            'violated_statements',
            'total_sitelinks',
            'wikipedia_sitelinks',
            'ores_score'
        ]), file=outputFile)

def printResults(batchOfResults, outputFileName):
    with open(outputFileName, 'a') as outputFile:
        for itemId, itemResults in batchOfResults.items():
            if('failed' in itemResults.keys()):
                continue

            # list of str-mapped values, delimited by OUTPUT_DELIMITER
            print(OUTPUT_DELIMITER.join(map(str, [
                itemId,
                itemResults['statements'],
                itemResults['violations_mandatory'],
                itemResults['violations_normal'],
                itemResults['violations_suggestion'],
                itemResults['violated_statements'],
                itemResults['total_sitelinks'],
                itemResults['wikipedia_sitelinks'],
                itemResults['ores_score'],
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
    # Returns a dictionary of items, each with their the number of statements
    batchOfResults = {}
    async with ClientSession() as session:
        async with session.get(STATEMENT_COUNT_URL + '&titles=' + '|'.join(itemIds)) as statementCountResponse:
            statementCountResponse = await statementCountResponse.read()
            r = json.loads(str(statementCountResponse, 'utf-8'))

    for page in r['query']['pages'].values():
        if not 'pageprops' in page:
            logErrorMessage("Item " + page['title'] + ' does not exist or is a redirect.')
            continue

        # add revid and number of statements to the item's results dictionary in batchOfResults
        results = {
            'revid': page['revisions'][0]['revid'],
            'statements': page['pageprops']['wb-claims'],
        }
        batchOfResults.update({page['title']: results})

    return batchOfResults

async def fetchNumberOfSitelinks(batchOfResults):
    # Gets a dictionary of itemIds and their statement count results
    # and adds to it the total number of sitelinks and the number of wikipedia sitelinks per itemId
    async with ClientSession() as session:
        async with session.get(SITELINK_COUNT_URL + '&ids=' + '|'.join(batchOfResults.keys())) as sitelinksResponse:
            sitelinksResponse = await sitelinksResponse.read()
            r = json.loads(str(sitelinksResponse, 'utf-8'))

    if not 'entities' in r:
        raise Exception("could not find sitelinks for items", batchOfResults.keys())

    for itemId, item in r['entities'].items():
        total_sitelinks = item['sitelinks']
        wikipedia_sitelinks = { k: v for k, v in total_sitelinks.items()
            if k.endswith('wiki') and not k in ['commonswiki', 'specieswiki'] }
        # add total and wikipedia sitelinks to the item's results dictionary in batchOfItems
        results = {'total_sitelinks': len(total_sitelinks), 'wikipedia_sitelinks': len(wikipedia_sitelinks)}
        batchOfResults[itemId].update(results)
    return batchOfResults

async def checkConstraints(batchOfResults):
    items = '|'.join(batchOfResults.keys())
    async with ClientSession() as session:
        async with session.get(CONSTRAINT_CHECK_URL + '&id=' + items) as r:
            if r.status != 200:
                raise Exception(
                    'wbcheckconstraint API returned status code ' +
                    str(r.status) + ' for item(s) ' +  items
                )

            r = await r.read()

    jsonResponse = json.loads(str(r, 'utf-8'))
    if 'error' in jsonResponse:
        raise Exception(
            'wbcheckconstraint API returned error \'' +
            jsonResponse['error']['code'] +
            '\' for items ' + items
        )
    for itemId in jsonResponse['wbcheckconstraints']:
        itemCheck = jsonResponse['wbcheckconstraints'][itemId]
        constraintCheckResults = parseItemCheck(itemCheck)
        batchOfResults[itemId].update(constraintCheckResults)

    return batchOfResults

def parseItemCheck(jsonConstraintCheckResponse):
    results = {
        'violations_mandatory': 0,
        'violations_normal': 0,
        'violations_suggestion': 0,
        'violated_statements': 0,
        'statement_is_violated': False
    }
    claims = jsonConstraintCheckResponse['claims']

    # claims is a list (not a dict) if it's empty... yikes.
    if not type(claims) is dict:
        # no statements -> no violations
        return results

    for (property_id, statement_group) in claims.items():
        for statement in statement_group:
            results['statement_is_violated'] = False

            violated_mainsnaks = statement['mainsnak']['results']
            for violated_mainsnak in violated_mainsnaks:
                results = countResults(violated_mainsnak['status'], results)

            if 'qualifiers' in statement.keys():
                qualifier_items = statement['qualifiers'].items()
                for (qualifier_property_id, qualifier_item) in qualifier_items:
                    for qualifier_constraint_check in qualifier_item:
                        qualifier_results = qualifier_constraint_check['results']
                        for qualifier_result in qualifier_results:
                            results = countResults(qualifier_result['status'], results)

            if 'references' in statement.keys():
                reference_items = statement['references']
                for reference_item in reference_items:
                    for (snak_property_id, reference_constraint_checks) in reference_item['snaks'].items():
                        for reference_constraint_check in reference_constraint_checks:
                            reference_results = reference_constraint_check['results']
                            for reference_result in reference_results:
                                results = countResults(reference_result['status'], results)

    del results['statement_is_violated']
    return results

def countResults(status, results):
    # ignore
    if status == 'bad-parameters':
        return results

    if status == 'violation':
        results['violations_mandatory'] += 1
    elif status == 'warning':
        results['violations_normal'] += 1
    elif status == 'suggestion':
        results['violations_suggestion'] += 1

    if results['statement_is_violated'] == False:
        results['statement_is_violated'] = True
        results['violated_statements'] += 1

    return results

async def checkQualityByBatch(batchOfItems):
    try:
        batchOfItems = await checkConstraints(batchOfItems)
    except Exception as ex:
        logErrorMessage("failed to check quality constraints on items " +
                        '|'.join(batchOfItems.keys()))
        logErrorMessage("now checking them one-by-one")
        logException(ex)
        for itemId, itemResults in batchOfItems.items():
            checkedItemResults = await checkQualityByItem(itemId, itemResults)
            batchOfItems[itemId].update(checkedItemResults)

    return batchOfItems

async def checkQualityByItem(itemId, itemResults):
    try:
        itemResults = await checkConstraints({itemId: itemResults})
    except Exception as ex:
        logErrorMessage("failed to check quality constraints on item " + itemId)
        logException(ex)
        return {'failed': True}

    return itemResults

async def fetchOresScore(batchOfItems):
    # collect Q-ids and revids from items dictionary
    itemIds = {}
    for itemId, results in batchOfItems.items():
        itemIds[str(results['revid'])] = itemId

    async with ClientSession() as session:
        async with session.get(ORES_URL + '|'.join(itemIds.keys())) as oresResponse:
            oresResponse = await oresResponse.read()
    r = json.loads(str(oresResponse, 'utf-8'))
    if not 'wikidatawiki' in r:
        logErrorMessage("no ORES scores found for items " + '|'.join(itemIds.keys()))
        return batchOfItems

    for revid, score in r['wikidatawiki']['scores'].items():
        itemId = itemIds[revid]
        probability = score['itemquality']['score']['probability']
        weightedSum = 0
        for x in probability:
            if(probability[x]):
                weightedSum += probability[x] * ORES_WEIGHTS[x]
        batchOfItems[itemId].update({'ores_score': round(weightedSum, 2)})

    return batchOfItems

async def main(argv):
    numberOfItems, outputFileName, inputFileName= parseArguments(argv)

    printHeader(outputFileName)

    if(numberOfItems):
        # we use randomly generated Q-IDs
        batchesOfItems = queryRandomItems(int(numberOfItems))
    else:
        # we read the Q-IDs from a file
        batchesOfItems = queryItemsFromFile(inputFileName)

    async for batch in batchesOfItems:
        itemsWithSitelinks = await fetchNumberOfSitelinks(batch)
        itemsWithConstraintChecks = await checkQualityByBatch(itemsWithSitelinks)
        itemsWithOresScore = await fetchOresScore(itemsWithConstraintChecks)
        printResults(itemsWithOresScore, outputFileName)
        print('', len(itemsWithOresScore))

    print()

loop=asyncio.get_event_loop()
loop.run_until_complete(main(sys.argv[1:]))
