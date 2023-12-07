from openskill.models import PlackettLuce
import json

model = PlackettLuce()

# Opening JSON file
f = open('testing_match_list.json')

data = json.load(f)

for i in data['data']:
    print(i['alliances'][0]['score'])
    print(i['alliances'][1]['score'])
    p1 = model.rating(name=i['alliances'][0]['teams'][0]['team']['name'])
    p2 = model.rating(name=i['alliances'][0]['teams'][1]['team']['name'])
    p3 = model.rating(name=i['alliances'][1]['teams'][0]['team']['name'])
    p4 = model.rating(name=i['alliances'][1]['teams'][1]['team']['name'])
    scores = [i['alliances'][0]['score'], i['alliances'][1]['score']]
    team1 = [p1, p2]
    team2 = [p3, p4]
    match = [team1, team2]
    [team1, team2] = model.rate(match, scores=scores)
    print(p1)
    print(p2)
    print(p3)
    print(p4)



# Closing file
f.close()
