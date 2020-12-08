import requests

URL = 'https://www.wikidata.org/w/api.php?format=json&action=wbcheckconstraints'
q_id = 'Q64'

r = requests.get(URL + '&id=' + q_id).json()

# https://www.wikidata.org/w/api.php?format=json&id=Q78469&action=wbcheckconstraints

for (property_id, constraint_checks) in r['wbcheckconstraints'][q_id]['claims'].items():
        for constraint_check in constraint_checks:
            main_results = constraint_check['mainsnak']['results']
            for main_result in main_results:
                print("property_id, status:", property_id, main_result['status'])

            if 'qualifiers' in constraint_check.keys():
                qualifier_items = constraint_check['qualifiers'].items()
                for (qualifier_property_id, qualifier_item) in qualifier_items:
                    for qualifier_constraint_check in qualifier_item:
                        qualifier_results = qualifier_constraint_check['results']
                        for qualifier_result in qualifier_results:
                            print("property_id, qualifier_property_id, status:", property_id, qualifier_property_id, qualifier_result['status'])





