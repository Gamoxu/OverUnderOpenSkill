import datetime
from dateutil.parser import parse
import glob
from io import StringIO
import json
import math
import numpy as np
import opr_calc as op
import os
import pandas as pd
import random
import requests
import time
import warnings

# We're going to set up the base URIs for the vexdb.io REST APIS. See:
# 
#     https://vexdb.io/the_data/
#     
# For the full documentation of the API

VEXDB_IO_GET_EVENTRANKINGS_URL = "https://api.vexdb.io/v1/get_rankings?sku={0}"
VEXDB_IO_GET_TEAM_SEASONRANKINGS_URL = "https://api.vexdb.io/v1/get_season_rankings?season={1}&program=VRC&team={0}"

# We're going to set up the base URIs for the RobotEvents.com REST APIS. See:
# 
#     https://www.robotevents.com/api/v2
#     
# For the full documentation of the API

ROBOTEVENTS_TOKEN = "ADD_TOKEN_HERE"
ROBOTEVENTS_TOKEN2 = "ADD_TOKEN_HERE"
ROBOTEVENTS_TOKEN3 = "ADD_TOKEN_HERE"
ROBOTEVENTS_TOKEN4 = "ADD_TOKEN_HERE"
ROBOTEVENTS_TOKEN5 = "ADD_TOKEN_HERE"
ROBOTEVENTS_TOKEN6 = "ADD_TOKEN_HERE"
ROBOTEVENTS_TOKEN7 = "ADD_TOKEN_HERE"
ROBOTEVENTS_TOKEN8 = "ADD_TOKEN_HERE"
ROBOTEVENTS_TOKEN9 = "ADD_TOKEN_HERE"

ROBOTEVENTS_HEADER = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN, "accept": "application/json"}
ROBOTEVENTS_HEADER2 = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN2, "accept": "application/json"}
ROBOTEVENTS_HEADER3 = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN3, "accept": "application/json"}
ROBOTEVENTS_HEADER4 = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN4, "accept": "application/json"}
ROBOTEVENTS_HEADER5 = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN5, "accept": "application/json"}
ROBOTEVENTS_HEADER6 = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN6, "accept": "application/json"}
ROBOTEVENTS_HEADER7 = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN7, "accept": "application/json"}
ROBOTEVENTS_HEADER8 = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN8, "accept": "application/json"}
ROBOTEVENTS_HEADER9 = {"Authorization": "Bearer " + ROBOTEVENTS_TOKEN9, "accept": "application/json"}

VEX_OVERUNDER_SEASON="Over Under"
VEX_SPINUP_SEASON="Spin Up"
VEX_TIPPINGPOINT_SEASON="Tipping Point"
VEX_CHANGEUP_SEASON="ChangeUp"
VEX_TOWERTAKEOVER_SEASON="Tower TakeOver"

VRC_PROGRAM_ID = 1 # From https://www.robotevents.com/api/v2/programs
VRC_OVER_UNDER_ID = 181 # From https://www.robotevents.com/api/v2/seasons?program[]=1
VRC_SPINUP_SEASON_ID = 173 # From https://www.robotevents.com/api/v2/seasons?program[]=1
VRC_TIPPINGPOINT_SEASON_ID = 154 # From https://www.robotevents.com/api/v2/seasons?program[]=1
VRC_CHANGEUP_SEASON_ID = 139 # From https://www.robotevents.com/api/v2/seasons?program[]=1
VRC_TOWERTAKEOVER_SEASON_ID = 130 # From https://www.robotevents.com/api/v2/seasons?program[]=1

# Results could have paging via this API
#ROBOT_EVENTS_SEASON_EVENTS = "https://www.robotevents.com/api/v2/seasons/{0}/events" # Parameter is SEASON_ID like 139
ROBOT_EVENTS_SEASON_EVENTS = "https://www.robotevents.com/api/v2/seasons/{0}/events?per_page=250" # Parameter is SEASON_ID like 139
ROBOT_EVENTS_EVENT_INFO = "https://www.robotevents.com/api/v2/events?sku[]={0}" # Parameter is SKU like RE-VRC-20-2012
ROBOT_EVENTS_EVENT_GET_DIVISIONS = "https://www.robotevents.com/api/v2/seasons/{0}/events?sku[]={1}" # Parameter is SEASON_ID like 139, then SKU
ROBOT_EVENTS_EVENT_GET_TEAMS = "https://www.robotevents.com/api/v2/events/{0}/teams" # Parameter is EVENT_ID like 42012
ROBOT_EVENTS_EVENT_GET_SKILLS = "https://www.robotevents.com/api/v2/events/{0}/skills" # Parameter is EVENT_ID like 42012
ROBOT_EVENTS_EVENT_GET_MATCHES_BY_DIVISION = "https://www.robotevents.com/api/v2/events/{0}/divisions/{1}/matches" # Parameter is EVENT_ID like 42012, Paremeter is DIVISION_ID like 1
ROBOT_EVENTS_EVENT_GET_RANKINGS_BY_DIVISION = "https://www.robotevents.com/api/v2/events/{0}/divisions/{1}/rankings" # Parameter is EVENT_ID like 42012, Paremeter is DIVISION_ID like 1
ROBOT_EVENTS_EVENT_GET_AWARDS = "https://www.robotevents.com/api/v2/events/{0}/awards"  # Parameter is EVENT_ID like 42012

ROBOT_EVENTS_GET_TEAM = "https://www.robotevents.com/api/v2/teams?program%5B%5D=1&number[]={0}" # Parameter is team number like 355V
ROBOT_EVENTS_TEAM_GET_MATCHES = "https://www.robotevents.com/api/v2/teams/{0}/matches?season[]={1}" # Parameter is TEAM_ID like 74980, then SEASON_ID
ROBOT_EVENTS_TEAM_GET_RANKINGS = "https://www.robotevents.com/api/v2/teams/{0}/rankings?season[]={1}" # Parameter is TEAM_ID, then SEASON_ID
ROBOT_EVENTS_TEAM_GET_SKILLS = "https://www.robotevents.com/api/v2/teams/{0}/skills?season[]={1}" # Parameter is TEAM_ID, then SEASON_ID
ROBOT_EVENTS_TEAM_GET_EVENTS = "https://www.robotevents.com/api/v2/teams/{0}/events?season[]={1}" # Parameter is TEAM_ID, then SEASON_ID
ROBOT_EVENTS_TEAM_GET_AWARDS = "https://www.robotevents.com/api/v2/teams/{0}/awards?season%5B%5D={1}" # Parameter is TEAM_ID, then SEASON_ID

ROBOT_EVENTS_TEAM_GET_REGISTERED_VRC = "https://www.robotevents.com/api/v2/teams?registered=true&program%5B%5D=1&grade%5B%5D=High%20School&grade%5B%5D=Middle%20School"

ROBOT_EVENTS_EVENT_URL = "https://www.robotevents.com/robot-competitions/vex-robotics-competition/{0}.html"

def get_event_info(sku):
    return _get_robotevents_data(ROBOT_EVENTS_EVENT_INFO.format(sku))

def get_season_events(season_id):
    return _get_robotevents_data(ROBOT_EVENTS_SEASON_EVENTS.format(season_id))

def get_event_divisions(season_id, sku):
    return _get_robotevents_data(ROBOT_EVENTS_EVENT_GET_DIVISIONS.format(season_id, sku))

def get_event_teams(event_id):
    return _get_robotevents_data(ROBOT_EVENTS_EVENT_GET_TEAMS.format(event_id))

def get_event_skills(event_id):
    return _get_robotevents_data(ROBOT_EVENTS_EVENT_GET_SKILLS.format(event_id))

def get_event_matches_by_division(event_id, division_id):
    return _get_robotevents_data(ROBOT_EVENTS_EVENT_GET_MATCHES_BY_DIVISION.format(event_id, division_id))

def get_event_rankings_by_division(event_id, division_id):
    return _get_robotevents_data(ROBOT_EVENTS_EVENT_GET_RANKINGS_BY_DIVISION.format(event_id, division_id))

def get_event_awards(event_id):
    return _get_robotevents_data(ROBOT_EVENTS_EVENT_GET_AWARDS.format(event_id))

def get_event_rankings_from_vexdbio(event_sku):
    return _get_vexdbio_data(VEXDB_IO_GET_EVENTRANKINGS_URL.format(event_sku))

def get_team_info(team_number):
    return _get_robotevents_data(ROBOT_EVENTS_GET_TEAM.format(team_number))

def get_team_events_season(season_id, team_id):
    return _get_robotevents_data(ROBOT_EVENTS_TEAM_GET_EVENTS.format(team_id, season_id))

def get_team_matches_by_season(season_id, team_id):
    return _get_robotevents_data(ROBOT_EVENTS_TEAM_GET_MATCHES.format(team_id, season_id))

def get_team_rankings_by_season(season_id, team_id):
    return _get_robotevents_data(ROBOT_EVENTS_TEAM_GET_RANKINGS.format(team_id, season_id))

def get_team_skills_season(season_id, team_id):
    return _get_robotevents_data(ROBOT_EVENTS_TEAM_GET_SKILLS.format(team_id, season_id))

def get_team_awards_by_season(season_id, team_id):
    return _get_robotevents_data(ROBOT_EVENTS_TEAM_GET_AWARDS.format(team_id, season_id))

def get_team_rankings_by_season_from_vexdbio(season_name, team_code):
    return _get_vexdbio_data(VEXDB_IO_GET_TEAM_SEASONRANKINGS_URL.format(team_code, season_name))

def get_team_current_registered_vrc():
    return _get_robotevents_data(ROBOT_EVENTS_TEAM_GET_REGISTERED_VRC)

def _get_vexdbio_data(the_url):
    df_retval = None
    response = requests.get(the_url,headers=ROBOTEVENTS_HEADER)
    json_results = json.loads(response.content.decode('utf-8'))
    df_retval = pd.read_json(json.dumps(json_results['result']))

    return df_retval

def _get_robotevents_data(the_url):
    df_retval = None
    got_data = False
    num_attempts = 0
    max_attemps = 4

    while num_attempts < max_attemps and got_data == False:
        try:
            headers = [ROBOTEVENTS_HEADER, ROBOTEVENTS_HEADER2, ROBOTEVENTS_HEADER3, ROBOTEVENTS_HEADER4,
                       ROBOTEVENTS_TOKEN5, ROBOTEVENTS_HEADER6, ROBOTEVENTS_HEADER7, ROBOTEVENTS_HEADER8,
                       ROBOTEVENTS_HEADER9]
            use_header = random.choice(headers)  # Randomly selects equal probability

            response = requests.get(the_url,headers=use_header)
            json_results = json.loads(response.content.decode('utf-8',"ignore"))
        except Exception as e:
            print("Exception in _get_robotevents_data: " + str(e),flush=True)
            num_attempts = num_attempts + 1
            if num_attempts < max_attemps:
                time.sleep(30 + min(35,(num_attempts - 1 ) * 15))
            continue

        try:
            df_retval = pd.read_json(StringIO(json.dumps(json_results['data'])))
            got_data = True
        except Exception as e:
            num_attempts = num_attempts + 1
            print("Exception " + str(e) + " with response: " + json.dumps(json_results),flush=True)
            if num_attempts < max_attemps:
                time.sleep(30 + min(35,(num_attempts - 1 ) * 15))

    if got_data:
        while json_results['meta']['next_page_url'] is not None:
            got_data = False
            while num_attempts < max_attemps and got_data == False:
                time.sleep(1)
                try:
                    response = requests.get(json_results['meta']['next_page_url'],headers=ROBOTEVENTS_HEADER)
                    json_results = json.loads(response.content.decode('utf-8',"ignore"))
                except:
                    json_results = dict()
                    
                if "data" in json_results.keys():
                    result_json = json.dumps(json_results['data'])
                    df_thispage = pd.read_json(StringIO(result_json))
                    df_retval = df_retval.append(df_thispage)
                    got_data = True
                else:
                    print("No element 'data' in response: " + json.dumps(json_results),flush=True)
                    print("\tHTTP Response: " + str(response),flush=True)
                    print("\tHTTP Response Headers: " + str(response.headers),flush=True)
                    num_attempts = num_attempts + 1
                    if num_attempts >= max_attemps:
                        raise Exception("No element 'data' in RobotEvents response after " + str(num_attempts) + " attempts.")
                    else:
                        print("Waiting " + str(30 + min(35,(num_attempts - 1 ) * 15)) + " seconds for next pull request",flush=True)
                        time.sleep(30 + min(35,(num_attempts - 1 ) * 15))
    else:
        raise Exception("Unable to get data.")
    return df_retval

def expand_rankings_for_auton_wp(df):
    if df is None:
        return None
    if df.shape[0] == 0:
        df_retval = df.copy()
        df_retval['awp'] = 0
        return df_retval
    df_retval = df.copy(deep=True)
    df_retval['awp'] = df_retval['wp'] - df_retval['ties'] - 2 * df_retval['wins']
    return df_retval

def expand_location_column(df, prefix=""):
    if df is None:
        return None
    if df.shape[0] == 0:
        df_retval = df.copy()
        df_retval['loc_city'] = None
        df_retval['loc_region'] = None
        df_retval['loc_country'] = None
        return df_retval
    df_loc_ps = df['location'].apply(pd.Series)
    df_loc_ps = df_loc_ps[['city','region','country']]
    df_loc_ps.rename(columns={'city':prefix + 'loc_city','region':prefix + 'loc_region','country':prefix + 'loc_country'},inplace=True)
    df_retval = pd.concat([df_loc_ps, df], axis = 1) #.drop('event', axis = 1)
    return df_retval

def expand_event_column(df):
    if df is None:
        return None
    if df.shape[0] == 0:
        df_retval = df.copy()
        df_retval['event_id'] = -1
        df_retval['event_name'] = None
        df_retval['event_code'] = None
        return df_retval
    df_event_ps = df['event'].apply(pd.Series)
    df_event_ps.rename(columns={'id':'event_id','name':'event_name','code':'event_code'},inplace=True)
    df_retval = pd.concat([df_event_ps, df], axis = 1) #.drop('event', axis = 1)
    return df_retval

def expand_division_column(df):
    if df is None:
        return None
    if df.shape[0] == 0:
        df_retval = df.copy()
        df_retval['division_id'] = -1
        df_retval['division_name'] = None
        return df_retval
    df_event_ps = df['division'].apply(pd.Series)
    df_event_ps = df_event_ps[['id','name']]
    df_event_ps.rename(columns={'id':'division_id','name':'division_name'},inplace=True)
    df_retval = pd.concat([df_event_ps, df], axis = 1) #.drop('event', axis = 1)
    return df_retval

def expand_team_column(df):
    if df is None:
        return None
    if df.shape[0] == 0:
        df_retval = df.copy()
        df_retval['team_id'] = -1
        df_retval['team_name'] = None
        df_retval['team_code'] = None
        return df_retval
    df_team_ps = df['team'].apply(pd.Series)
    df_team_ps.rename(columns={'id':'team_id','name':'team_name','code':'team_code'},inplace=True)
    df_retval = pd.concat([df_team_ps, df], axis = 1) #.drop('team', axis = 1)
    return df_retval

def expand_teamwinners_column(df):
    if df is None:
        return None
    if df.shape[0] == 0:
        df_retval = df.copy()
        df_retval['winning_team_codes'] = None
        return df_retval
    df_retval = df.explode('teamWinners')
    df_retval['winning_team'] = ''
    df_retval.reset_index(inplace=True)
    for index in range(len(df_retval)):
        w = df_retval.iloc[index]['teamWinners']
        try:
            df_retval.loc[index,'winning_team'] = w['team']['name']
        except:
            pass
        
    return df_retval

def expand_award_qualifies_for_column(df):
    if df is None:
        return None
    if df.shape[0] == 0:
        df_retval = df.copy()
        df_retval['event_id'] = -1
        df_retval['event_name'] = None
        df_retval['event_code'] = None
        return df_retval
    df_event_ps = df['qualifications'].apply(pd.Series)
    if df_event_ps.shape[1] == 0:
        df_event_ps['qualifies_for'] = ""
    else:
        if len(df_event_ps.columns) == 1:
            df_event_ps.columns = ['qualifies_for']
        else:
            df_tmp = None
            q_list = ""
            for i, row in df_event_ps.iterrows():
                for r in row:
                    if q_list == "":
                        q_list = r
                    else:
                        q_list =q_list + "," + r

            d = dict()
            d['qualifies_for'] = q_list
            df_tmp = pd.DataFrame.from_dict([d])
            df_event_ps = df_tmp.copy()

    df_event_ps['qualifies_for'] = df_event_ps['qualifies_for'].apply(lambda x: x if isinstance(x, str) else "")
    df_retval = pd.concat([df_event_ps, df], axis = 1) #.drop('event', axis = 1)
    return df_retval

def expand_alliance_column(df):
    if df is None:
        return None
    if df.shape[0] == 0:
        df_retval = df.copy()
        df_retval['blue_score'] = None
        df_retval['blue_team1_id'] = None
        df_retval['blue_team1_code'] = None
        df_retval['blue_team2_id'] = None
        df_retval['blue_team2_code'] = None
        df_retval['red_score'] = None
        df_retval['red_team1_id'] = None
        df_retval['red_team1_code'] = None
        df_retval['red_team2_id'] = None
        df_retval['red_team2_code'] = None
        return df_retval
    df_retval = df.copy()
    df_retval['blue_score'] = 0
    df_retval['blue_team1_id'] = -1
    df_retval['blue_team1_code'] = None
    df_retval['blue_team2_id'] = -1
    df_retval['blue_team2_code'] = None
    df_retval['red_score'] = 0
    df_retval['red_team1_id'] = -1
    df_retval['red_team1_code'] = None
    df_retval['red_team2_id'] = -1
    df_retval['red_team2_code'] = None

    for index in range(len(df_retval)):
        alliances = df_retval.iloc[index]['alliances']
        if alliances[0]['color'] == "blue":
            df_retval.iloc[index,df_retval.columns.get_loc('blue_score')] = alliances[0]['score']
            try:
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team1_id')] = alliances[0]['teams'][0]['team']['id']
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team1_code')] = alliances[0]['teams'][0]['team']['name']
            except:
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team1_id')] = -1
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team1_code')] = ""
                
            try:
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team2_id')] = alliances[0]['teams'][1]['team']['id']
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team2_code')] = alliances[0]['teams'][1]['team']['name']
            except:
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team2_id')] = -1
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team2_code')] = ""
            
            df_retval.iloc[index,df_retval.columns.get_loc('red_score')] = alliances[1]['score']
            try:
                df_retval.iloc[index,df_retval.columns.get_loc('red_team1_id')] = alliances[1]['teams'][0]['team']['id']
                df_retval.iloc[index,df_retval.columns.get_loc('red_team1_code')] = alliances[1]['teams'][0]['team']['name']
            except:
                df_retval.iloc[index,df_retval.columns.get_loc('red_team1_id')] = -1
                df_retval.iloc[index,df_retval.columns.get_loc('red_team1_code')] = ""

            try:
                df_retval.iloc[index,df_retval.columns.get_loc('red_team2_id')] = alliances[1]['teams'][1]['team']['id']
                df_retval.iloc[index,df_retval.columns.get_loc('red_team2_code')] = alliances[1]['teams'][1]['team']['name']
            except:
                df_retval.iloc[index,df_retval.columns.get_loc('red_team2_id')] = -1
                df_retval.iloc[index,df_retval.columns.get_loc('red_team2_code')] = ""
        else:
            df_retval.iloc[index,df_retval.columns.get_loc('blue_score')] = alliances[1]['score']
            try:
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team1_id')] = alliances[1]['teams'][0]['team']['id']
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team1_code')] = alliances[1]['teams'][0]['team']['name']
            except:
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team1_id')] = -1
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team1_code')] = ""
            try:
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team2_id')] = alliances[1]['teams'][1]['team']['id']
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team2_code')] = alliances[1]['teams'][1]['team']['name']
            except:
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team2_id')] = -1
                df_retval.iloc[index,df_retval.columns.get_loc('blue_team2_code')] = ""
                
            df_retval.iloc[index,df_retval.columns.get_loc('red_score')] = alliances[0]['score']
            
            try:
                df_retval.iloc[index,df_retval.columns.get_loc('red_team1_id')] = alliances[0]['teams'][0]['team']['id']
                df_retval.iloc[index,df_retval.columns.get_loc('red_team1_code')] = alliances[0]['teams'][0]['team']['name']
            except:
                df_retval.iloc[index,df_retval.columns.get_loc('red_team1_id')] = -1
                df_retval.iloc[index,df_retval.columns.get_loc('red_team1_code')] = ""
            try:
                df_retval.iloc[index,df_retval.columns.get_loc('red_team2_id')] = alliances[0]['teams'][1]['team']['id']
                df_retval.iloc[index,df_retval.columns.get_loc('red_team2_code')] = alliances[1]['teams'][1]['team']['name']
            except:
                df_retval.iloc[index,df_retval.columns.get_loc('red_team2_id')] = -1
                df_retval.iloc[index,df_retval.columns.get_loc('red_team2_code')] = ""
            
    return df_retval

def return_alliances_from_event_matches(df_matches):
    """ Returns the alliances found for a particular event

        This assumes the Pandas Dataframe passed in contains all
        matches for a single Event.
        
        Round 2 is for Qualification
        Round 7 is the Round of 32
        Round 6 is the Round of 16
        Round 3 is the Quarter Finals
        Round 4 is the Semi Finals
        Round 5 is the Finals

        Alliance seed is implied by which R16 match and color a team is

        For example (Higher seed is always red):
            R16 #1 is 1v16 / #2 is 8v9  / #3 is 4v13 / #4 is 5v12
            R16 #5 is 2v15 / #6 is 7v10 / #7 is 3v14 / #8 is 6v11

        If there are fewer than 16 Alliances, the top several teams have byes
            QF #1 is 1v8 / #2 is 4v5 / #3 is 2v7 / #4 is 3v6
            SF #1 is 1v4 / #2 is 2v3
            F        1v2
    """
    if df_matches is None:
        return None
    df = df_matches.copy(deep=True)
    df['instance'] = df['instance'].astype('str')
    df_alliances = None

    df_alliance1 = df.query("round == 6 and ((instance == '1' or instance == '1-1') or instance == '1-2')")
    if len(df_alliance1) == 0:
        df_alliance1 = df.query("round == 3  and ((instance == '1' or instance == '1-1') or instance == '1-2')")
    if len(df_alliance1) == 0:
        df_alliance1 = df.query("round == 4  and ((instance == '1' or instance == '1-1') or instance == '1-2')")
    if len(df_alliance1) == 0:
        df_alliance1 = df.query("round == 5  and ((instance == '1' or instance == '1-1') or instance == '1-2')")
    df_alliance1 = df_alliance1[['sku','red_team1_code','red_team2_code']]
    df_alliance1.rename(columns={"red_team1_code":"captain","red_team2_code":"pick"},inplace=True)
    df_alliance1['seed'] = 1
    df_alliance1 = df_alliance1.set_index('seed')
    if df_alliances is None:
        df_alliances = df_alliance1
    else:
        df_alliances = df_alliances.append(df_alliance1)
    
    df_alliance2 = df.query("round == 6 and ((instance == '5' or instance == '5-1') or instance == '5-2')")
    if len(df_alliance2) == 0:
        df_alliance2 = df.query("round == 3  and ((instance == '3' or instance == '3-1') or instance == '3-2')")
    if len(df_alliance2) == 0:
        df_alliance2 = df.query("round == 4  and ((instance == '2' or instance == '2-1') or instance == '2-2')")
    df_alliance2 = df_alliance2[['sku','red_team1_code','red_team2_code']]
    df_alliance2.rename(columns={"red_team1_code":"captain","red_team2_code":"pick"},inplace=True)
    df_alliance2['seed'] = 2
    df_alliance2 = df_alliance2.set_index('seed')
    df_alliances = df_alliances.append(df_alliance2)
    
    df_alliance3 = df.query("round == 6 and ((instance == '7' or instance == '7-1') or instance == '7-2')")
    if len(df_alliance3) == 0:
        df_alliance3 = df.query("round == 3  and ((instance == '4' or instance == '4-1') or instance == '4-2')")
    if len(df_alliance3) == 0:
        df_alliance3 = df.query("round == 4  and ((instance == '2' or instance == '2-1') or instance == '2-2')")
        df_alliance3 = df_alliance3[['sku','blue_team1_code','blue_team2_code']]
        df_alliance3.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    else:
        df_alliance3 = df_alliance3[['sku','red_team1_code','red_team2_code']]
        df_alliance3.rename(columns={"red_team1_code":"captain","red_team2_code":"pick"},inplace=True)
    df_alliance3['seed'] = 3
    df_alliance3 = df_alliance3.set_index('seed')
    df_alliances = df_alliances.append(df_alliance3)

    df_alliance4 = df.query("round == 6 and ((instance == '3' or instance == '3-1') or instance == '3-2')")
    if len(df_alliance4) == 0:
        df_alliance4 = df.query("round == 3  and ((instance == '2' or instance == '2-1') or instance == '2-2')")
    if len(df_alliance4) == 0:
        df_alliance4 = df.query("round == 4  and ((instance == '1' or instance == '1-1') or instance == '1-2')")
        df_alliance4 = df_alliance4[['sku','blue_team1_code','blue_team2_code']]
        df_alliance4.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    else:
        df_alliance4 = df_alliance4[['sku','red_team1_code','red_team2_code']]
        df_alliance4.rename(columns={"red_team1_code":"captain","red_team2_code":"pick"},inplace=True)
    df_alliance4['seed'] = 4
    df_alliance4 = df_alliance4.set_index('seed')
    df_alliances = df_alliances.append(df_alliance4)
    
    df_alliance5 = df.query("round == 6 and ((instance == '4' or instance == '4-1') or instance == '4-2')")
    if len(df_alliance5) == 0:
        df_alliance5 = df.query("round == 3 and ((instance == '2' or instance == '2-1') or instance == '2-2')")
        df_alliance5 = df_alliance5[['sku','blue_team1_code','blue_team2_code']]
        df_alliance5.rename(columns={'blue_team1_code':'captain',"blue_team2_code":"pick"},inplace=True)
    else:
        df_alliance5 = df_alliance5[['sku','red_team1_code','red_team2_code']]
        df_alliance5.rename(columns={"red_team1_code":"captain","red_team2_code":"pick"},inplace=True)
    df_alliance5['seed'] = 5
    df_alliance5 = df_alliance5.set_index('seed')
    df_alliances = df_alliances.append(df_alliance5)

    df_alliance6 = df.query("round == 6 and ((instance == '8' or instance == '8-1') or instance == '8-2')")
    if len(df_alliance6) == 0:
        df_alliance6 = df.query("round == 3 and ((instance == '4' or instance == '4-1') or instance == '4-2')")
        df_alliance6 = df_alliance6[['sku','blue_team1_code','blue_team2_code']]
        df_alliance6.rename(columns={'blue_team1_code':'captain','blue_team2_code':'pick'},inplace=True)
    else:
        df_alliance6 = df_alliance6[['sku','red_team1_code','red_team2_code']]
        df_alliance6.rename(columns={"red_team1_code":"captain","red_team2_code":"pick"},inplace=True)
    df_alliance6['seed'] = 6
    df_alliance6 = df_alliance6.set_index('seed')
    df_alliances = df_alliances.append(df_alliance6)

    df_alliance7 = df.query("round == 6 and ((instance == '6' or instance == '6-1') or instance == '6-2')")
    if len(df_alliance7) == 0:
        df_alliance7 = df.query("round == 3 and ((instance == '3' or instance == '3-1') or instance == '3-2')")
        df_alliance7 = df_alliance7[['sku','blue_team1_code','blue_team2_code']]
        df_alliance7.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    else:
        df_alliance7 = df_alliance7[['sku','red_team1_code','red_team2_code']]
        df_alliance7.rename(columns={"red_team1_code":"captain","red_team2_code":"pick"},inplace=True)
    df_alliance7['seed'] = 7
    df_alliance7 = df_alliance7.set_index('seed')
    df_alliances = df_alliances.append(df_alliance7)

    df_alliance8 = df.query("round == 6 and ((instance == '2' or instance == '2-1') or instance == '2-2')")
    if len(df_alliance8) == 0:
        df_alliance8 = df.query("round == 3 and ((instance == '1' or instance == '1-1') or instance == '1-2')")
        df_alliance8 = df_alliance8[['sku','blue_team1_code','blue_team2_code']]
        df_alliance8.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    else:
        df_alliance8 = df_alliance8[['sku','red_team1_code','red_team2_code']]
        df_alliance8.rename(columns={"red_team1_code":"captain","red_team2_code":"pick"},inplace=True)
    df_alliance8['seed'] = 8
    df_alliance8 = df_alliance8.set_index('seed')
    df_alliances = df_alliances.append(df_alliance8)

    df_alliance9 = df.query("round == 6 and ((instance == '2' or instance == '2-1') or instance == '2-2')")
    df_alliance9 = df_alliance9[['sku','blue_team1_code','blue_team2_code']]
    df_alliance9.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    df_alliance9['seed'] = 9
    df_alliance9 = df_alliance9.set_index('seed')
    df_alliances = df_alliances.append(df_alliance9)
    
    df_alliance10 = df.query("round == 6 and ((instance == '6' or instance == '6-1') or instance == '6-2')")
    df_alliance10 = df_alliance10[['sku','blue_team1_code','blue_team2_code']]
    df_alliance10.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    df_alliance10['seed'] = 10
    df_alliance10 = df_alliance10.set_index('seed')
    df_alliances = df_alliances.append(df_alliance10)

    df_alliance11 = df.query("round == 6 and ((instance == '8' or instance == '8-1') or instance == '8-2')")
    df_alliance11 = df_alliance11[['sku','blue_team1_code','blue_team2_code']]
    df_alliance11.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    df_alliance11['seed'] = 11
    df_alliance11 = df_alliance11.set_index('seed')
    df_alliances = df_alliances.append(df_alliance11)
    
    df_alliance12 = df.query("round == 6 and ((instance == '4' or instance == '4-1') or instance == '4-2')")
    df_alliance12 = df_alliance12[['sku','blue_team1_code','blue_team2_code']]
    df_alliance12.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    df_alliance12['seed'] = 12
    df_alliance12 = df_alliance12.set_index('seed')
    df_alliances = df_alliances.append(df_alliance12)
    
    df_alliance13 = df.query("round == 6 and ((instance == '3' or instance == '3-1') or instance == '3-2')")
    df_alliance13 = df_alliance13[['sku','blue_team1_code','blue_team2_code']]
    df_alliance13.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    df_alliance13['seed'] = 13
    df_alliance13 = df_alliance13.set_index('seed')
    df_alliances = df_alliances.append(df_alliance13)

    df_alliance14 = df.query("round == 6 and ((instance == '7' or instance == '7-1') or instance == '7-2')")
    df_alliance14 = df_alliance14[['sku','blue_team1_code','blue_team2_code']]
    df_alliance14.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    df_alliance14['seed'] = 14
    df_alliance14 = df_alliance14.set_index('seed')
    df_alliances = df_alliances.append(df_alliance14)
    
    df_alliance15 = df.query("round == 6 and ((instance == '5' or instance == '5-1') or instance == '5-2')")
    df_alliance15 = df_alliance15[['sku','blue_team1_code','blue_team2_code']]
    df_alliance15.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    df_alliance15['seed'] = 15
    df_alliance15 = df_alliance15.set_index('seed')
    df_alliances = df_alliances.append(df_alliance15)
    
    df_alliance16 = df.query("round == 6 and ((instance == '1' or instance == '1-1') or instance == '1-2')")
    df_alliance16 = df_alliance16[['sku','blue_team1_code','blue_team2_code']]
    df_alliance16.rename(columns={"blue_team1_code":"captain","blue_team2_code":"pick"},inplace=True)
    df_alliance16['seed'] = 16
    df_alliance16 = df_alliance16.set_index('seed')
    df_alliances = df_alliances.append(df_alliance16)
    df_alliances.drop_duplicates(inplace=True)
    
    return df_alliances

def decorate_matches(df, df_alliances, is_finals_division=False):
    """ Returns the alliances found for a particular event

        This assumes the Pandas Dataframe passed in contains all
        matches for a single Event.

        The second Dataframe should be the alliances, as calculated
        by the return_alliances_from_event_matches function.
        
        Round 2 is for Qualification
        Round 7 is the Round of 32
        Round 6 is the Round of 16
        Round 3 is the Quarter Finals
        Round 4 is the Semi Finals
        Round 5 is the Finals

        Alliance seed is implied by which R16 match and color a team is

        For example (Higher seed is always red):
            R16 #1 is 1v16 / #2 is 8v9  / #3 is 4v13 / #4 is 5v12
            R16 #5 is 2v15 / #6 is 7v10 / #7 is 3v14 / #8 is 6v11

        If there are fewer than 16 Alliances, the top several teams have byes
            QF #1 is 1v8 / #2 is 4v5 / #3 is 2v7 / #4 is 3v6
            SF #1 is 1v4 / #2 is 2v3
            F        1v2
    """
    if df is None:
        return None
    df_retval = df.copy(deep=True)
    df_retval['winner'] = "Tie"
    df_retval.loc[df_retval['red_score'] > df_retval['blue_score'],'winner'] = "Red"
    df_retval.loc[df_retval['red_score'] < df_retval['blue_score'],'winner'] = "Blue"
    
    df_retval['redwins'] = 0
    df_retval['bluewins'] = 0
    df_retval['ties'] = 0
    df_retval.loc[(df_retval['winner'] =="Blue"),'bluewins'] = 1
    df_retval.loc[(df_retval['winner'] =="Red"),'redwins'] = 1
    df_retval.loc[(df_retval['winner'] =="Tie"),'ties'] = 1
    
    df_retval['match_type'] = "0_Unknown"
    df_retval['elimination_round'] = ""
    df_retval['R16_seed'] = ""
    
    df_retval.loc[(df_retval['round'] == 7),'elimination_round'] = "1_R32"
    df_retval.loc[(df_retval['round'] == 6),'elimination_round'] = "2_R16"
    df_retval.loc[(df_retval['round'] == 6) & (df_retval['instance'] == 1),'R16_seed'] = "1_v_16"
    df_retval.loc[(df_retval['round'] == 6) & (df_retval['instance'] == 2),'R16_seed'] = "8_v_9"
    df_retval.loc[(df_retval['round'] == 6) & (df_retval['instance'] == 3),'R16_seed'] = "4_v_13"
    df_retval.loc[(df_retval['round'] == 6) & (df_retval['instance'] == 4),'R16_seed'] = "5_v_12"
    df_retval.loc[(df_retval['round'] == 6) & (df_retval['instance'] == 5),'R16_seed'] = "2_v_15"
    df_retval.loc[(df_retval['round'] == 6) & (df_retval['instance'] == 6),'R16_seed'] = "7_v_10"
    df_retval.loc[(df_retval['round'] == 6) & (df_retval['instance'] == 7),'R16_seed'] = "3_v_14"
    df_retval.loc[(df_retval['round'] == 6) & (df_retval['instance'] == 8),'R16_seed'] = "6_v_11"
    df_retval.loc[(df_retval['round'] == 3),'elimination_round'] = "3_QF"
    df_retval.loc[(df_retval['round'] == 3) & (df_retval['instance'] == 1),'R16_seed'] = "1_v_8"
    df_retval.loc[(df_retval['round'] == 3) & (df_retval['instance'] == 2),'R16_seed'] = "4_v_5"
    df_retval.loc[(df_retval['round'] == 3) & (df_retval['instance'] == 3),'R16_seed'] = "2_v_7"
    df_retval.loc[(df_retval['round'] == 3) & (df_retval['instance'] == 4),'R16_seed'] = "3_v_6"
    df_retval.loc[(df_retval['round'] == 4),'elimination_round'] = "4_SF"
    df_retval.loc[(df_retval['round'] == 4) & (df_retval['instance'] == 1),'R16_seed'] = "1_v_4"
    df_retval.loc[(df_retval['round'] == 4) & (df_retval['instance'] == 2),'R16_seed'] = "2_v_3"
    df_retval.loc[(df_retval['round'] == 5),'elimination_round'] = "5_F"
    df_retval.loc[(df_retval['round'] == 5),'R16_seed'] = "1_v_2"

    df_retval.loc[(df_retval['round'] == 2),'match_type'] = "1_Qualifier"
    df_retval.loc[(df_retval['round'] > 2) & (df_retval['round'] < 7),'match_type'] = "2_Elimination"

    if df_alliances is None:
        df_retval['red_seed'] = None
        df_retval['blue_seed'] = None
    else:
        if is_finals_division:
            for rtc in df_retval['red_team1_code'].unique():
                df_retval.loc[df_retval['red_team1_code'] == rtc, "red_seed"] = df_alliances.query("captain=='" + rtc +"'")['seed'].min()
            for btc in df_retval['blue_team1_code'].unique():
                df_retval.loc[df_retval['blue_team1_code'] == btc, "blue_seed"] = df_alliances.query("captain=='" + btc +"'")['seed'].min()
        else:
            for i in range(1, 33):
                df_seed = df_alliances.query("seed==" + str(i))
                if len(df_seed) == 1:
                    df_retval.loc[(df_retval['round'] > 2) & (df_retval['red_team1_code'] == df_seed.captain.max()),'red_seed'] = i
                    df_retval.loc[(df_retval['round'] > 2) & (df_retval['blue_team1_code'] == df_seed.captain.max()),'blue_seed'] = i
    
    return df_retval

def event_matches_pivot_to_team_matches(df_matches):
    """ Quadruples the size of df_matches by making it oriented by team

        Output columns:     
          ['sku','division_id','round_num','elimination_round','instance','matchnum','started',
          'team_code','partner_code','opp1_code','opp2_code',
          'red_score','blue_score','team_seed','opp_seed','WLT','margin']
    """
    column_subset = ['sku','division_id','round_num','elimination_round','instance','matchnum','started','red_team1_code','red_team2_code','blue_team1_code','blue_team2_code','red_score','blue_score','red_seed','blue_seed']
    if 'video_url' in df_matches.columns:
        column_subset = ['sku','division_id','round_num','elimination_round','instance','matchnum','started','red_team1_code','red_team2_code','blue_team1_code','blue_team2_code','red_score','blue_score','red_seed','blue_seed','video_url']
    df_red1 = df_matches[column_subset]
    df_red2 = df_matches[column_subset]
    df_blue1 = df_matches[column_subset]
    df_blue2 = df_matches[column_subset]

    df_red1.rename(columns={'red_team1_code':'team_code',
                            'red_team2_code':'partner_code',
                            'red_seed':'team_seed',
                            'blue_team1_code':'opp1_code',
                            'blue_team2_code':'opp2_code',
                            'blue_seed':'opp_seed',
                            'red_score':'team_score',
                            'blue_score':'opp_score'},inplace=True)
    df_red2.rename(columns={'red_team2_code':'team_code',
                            'red_team1_code':'partner_code',
                            'red_seed':'team_seed',
                            'blue_team1_code':'opp1_code',
                            'blue_team2_code':'opp2_code',
                            'blue_seed':'opp_seed',
                            'red_score':'team_score',
                            'blue_score':'opp_score'},inplace=True)
    df_blue1.rename(columns={'blue_team1_code':'team_code',
                             'blue_team2_code':'partner_code',
                             'blue_seed':'team_seed',
                             'red_team1_code':'opp1_code',
                             'red_team2_code':'opp2_code',
                             'red_seed':'opp_seed',
                             'blue_score':'team_score',
                             'red_score':'opp_score'},inplace=True)
    df_blue2.rename(columns={'blue_team2_code':'team_code',
                             'blue_team1_code':'partner_code',
                             'blue_seed':'team_seed',
                             'red_team1_code':'opp1_code',
                             'red_team2_code':'opp2_code',
                             'red_seed':'opp_seed',
                             'blue_score':'team_score',
                             'red_score':'opp_score'},inplace=True)
    
    df_team_matches = df_red1.append(df_red2).append(df_blue1).append(df_blue2)
    df_team_matches.loc[df_team_matches['team_score'] == df_team_matches['opp_score'],'WLT'] = '3_Tie'
    df_team_matches.loc[df_team_matches['team_score'] > df_team_matches['opp_score'],'WLT'] = '1_Win'
    df_team_matches.loc[df_team_matches['team_score'] < df_team_matches['opp_score'],'WLT'] = '2_Loss'
    df_team_matches['margin'] = df_team_matches['team_score'] - df_team_matches['opp_score']
    df_team_matches['margin'] = df_team_matches['margin'].astype(int)
    return df_team_matches

def process_event_and_save_to_file(row, output_path, is_league_event=False):
    """ Processes event results/rankings
    
    The `row` parameter is expected to be a dataframe row from `get_season_events`
    with the `event_name` field remapped from `name` and the location field expanded

    The `output_path` is a directory that already exists

    The `is_league_event` is whether the event is a League event or normal tournament

    Persists the following files:
        * row['sku'] + "_Skills.csv" normally
        * row['sku'] + "_Division" + str(d['id']) + "_Alliances.csv" (for each Division)
        * row['sku'] + "_Rankings.csv" normally
        * row['sku'] + "_" + datetime.date.today().strftime('%Y_%m_%d') + "_LeagueRankings.csv" (League)
        * Matches file:
          * row['sku'] + "_Matches.csv" normally
          * row['sku'] + "_" datetime.date.today().strftime('%Y_%m_%d') + "_LeagueMatches.csv" (League)
    """
    event_matches = None
    event_rankings = None
    event_alliances = None
    print('Event: ' + row['event_name'])

    # Try to get skills
    event_skills = None
    try:
        event_skills = get_event_skills(row['id'])
        print("\tFound " + str(event_skills.shape[0]) + " skills records")
        event_skills = expand_team_column(event_skills)
        event_skills = expand_event_column(event_skills)
        if event_skills is not None:        
            try:
                os.remove(output_path + row['sku'] + "_Skills.csv")
                time.sleep(1)
            except:
                pass
        
            event_skills.to_csv(output_path + row['sku'] + "_Skills.csv", index=False)
            print("\tSaved skills")
    except:
        pass

    for d in row['divisions']:
        print('  Division: ' + d['name'])
        try:
            event_division_results = get_event_rankings_by_division(row['id'],d['id'])
            #if event_division_results.shape[0] == 0 and d['id'] == 1: # Finals Divisions do not have rankings
            #    print("  Skipping - no results")
            #    continue
        except Exception as e:
            print("   - Problem: " + str(e))
            continue
        event_division_results = expand_event_column(event_division_results)
        event_division_results = expand_division_column(event_division_results)
        event_division_results = expand_team_column(event_division_results)
        event_division_results = expand_rankings_for_auton_wp(event_division_results)
        if event_rankings is None:
            event_rankings = event_division_results
        else:
            event_rankings = event_rankings.append(event_division_results)
        
        try:
            event_division_matches = get_event_matches_by_division(row['id'],d['id'])
            if event_division_matches.shape[0] == 0:
                print("   Skipping - no division matches")
                continue
        except Exception as e:
            print("Exception getting matches - " + str(e))
            continue
        #else:
        #    print("   Found " + str(event_division_matches.shape[0]) + " matches")
        #print("Columns " + str(event_division_matches.columns))
        event_division_matches = expand_event_column(event_division_matches)
        event_division_matches = expand_division_column(event_division_matches)
        event_division_matches = expand_alliance_column(event_division_matches)
        # Added 10/22/2021
        event_division_matches['started'] = np.where(event_division_matches['started'].isnull(),event_division_matches['scheduled'],event_division_matches['started'])
        # print("   After expanding fields, now have " + str(event_division_matches.shape[0]) + " matches")

        event_division_alliances = None
        try:
            if "final" not in d['name'].lower() and "championship" not in d['name'].lower():
                event_division_alliances = return_alliances_from_event_matches(event_division_matches.rename(columns={"event_code":"sku"}))
                if event_alliances is None:
                    event_alliances = event_division_alliances
                else:
                    event_alliances = event_alliances.append(event_division_alliances)

                if 'rank' in event_division_results.columns:
                    min_edr = event_division_results[['team_name','rank']].copy(deep=True)
                    event_division_alliances_ranks = event_division_alliances.join(min_edr.set_index('team_name'),on='captain',how='left').rename(columns={'rank':'captain_rank'})
                    event_division_alliances_ranks = event_division_alliances_ranks.join(min_edr.set_index('team_name'),on='pick',how='left').rename(columns={'rank':'pick_rank'})

                    try:
                        os.remove(output_path + row['sku'] + "_Division" + str(d['id']) + "_Alliances.csv")
                    except:
                        pass
                    event_division_alliances_ranks.drop_duplicates(inplace=True)
                    event_division_alliances_ranks.to_csv(output_path + row['sku'] + "_Division" + str(d['id']) + "_Alliances.csv", index=True)
                else:
                    print("  Skipping because no 'rank' field available yet.")
                    event_division_alliances = None
            else:
                print("   Skipping Alliance work for Finals Division")
        except Exception as e:
            print("   Problem with alliances - " + str(e))
            event_division_alliances = None
        
        try:
            try:
                if d['id'] < len(row['divisions']) and len(row['divisions']) > 1: # Finals division
                    event_all_alliances = pd.concat(map(pd.read_csv, glob.glob(os.path.join(output_path, row['sku'] + "*_Alliances.csv"))))
                    event_division_matches = decorate_matches(event_division_matches, event_all_alliances, True)
                else:
                    event_division_matches = decorate_matches(event_division_matches, event_division_alliances)
            except Exception as ed:
                print("   Problem with decorating matches with alliance data, trying without it next - " + str(ed))
                try:
                    event_division_matches = decorate_matches(event_division_matches, None)
                except Exception as ed1:
                    print("   Problem with decorating matches after avoiding alliance data - " + str(ed))

            if event_matches is None:
                event_matches = event_division_matches
            else:
                event_matches = event_matches.append(event_division_matches)
        except Exception as e:
            print("   Problem with matches - " + str(e))

    if event_matches is not None:
        try:
            if is_league_event:
                event_matches.to_csv(output_path + row['sku'] + "_" + datetime.date.today().strftime('%Y_%m_%d') + "_LeagueMatches.csv", index=False)
                # Get all league matches
                event_matches = pd.concat(map(pd.read_csv, glob.glob(os.path.join(output_path, row['sku'] + "_*_LeagueMatches.csv"))))
                event_matches.drop_duplicates(inplace=True)
            else:
                event_matches.to_csv(output_path + row['sku'] + "_Matches.csv", index=False)

            df_teams = event_rankings[['team_id','team_name']].rename(columns={"team_name":"number","team_id":"id"})
            event_qualifier_matches = event_matches.rename(columns={"round":"round_num"}).query("round_num==2")
            
            event_opr = op.calculate_opr_ccwm(event_qualifier_matches.query("red_score+blue_score>0"), df_teams)
            event_rankings = event_rankings.join(event_opr.set_index("team_id"),on="team_id")
            
            df_trueskill_rating, trueskills = op.calculate_trueskill(event_matches.rename(columns={"round":"round_num"}).query("red_score+blue_score>0"), df_teams)
            event_rankings = event_rankings.join(df_trueskill_rating.set_index("team_code"),on="team_name")
            
            # df_matches_by_team = event_matches_pivot_to_team_matches(event_matches.rename(columns={"round":"elimination_round",'event_id':'sku'}))

            try:
                os.remove(output_path + row['sku'] + "_Rankings.csv")
            except:
                pass

            if is_league_event:
                event_rankings.to_csv(output_path + row['sku'] + "_" + datetime.date.today().strftime('%Y_%m_%d') + "_LeagueRankings.csv", index=False)
                event_rankings.to_csv(output_path + row['sku'] + "_Rankings.csv", index=False)
            else:
                event_rankings.to_csv(output_path + row['sku'] + "_Rankings.csv", index=False)

            try:
                os.remove(output_path + row['sku'] + "_TeamCounts.csv")
            except:
                pass

            dict_event_team_count = {
                'sku':row['sku'],
                'team_count':int(event_rankings.shape[0]),
            }
            df_event_team_count = pd.DataFrame.from_dict([dict_event_team_count])
            df_event_team_count.to_csv(output_path + row['sku'] + "_TeamCounts.csv", index=False)

        except Exception as e:
            print("  Problem matches - " + str(e))
    else:
        print("  Could not find event matches for " + row['sku'] + ' ' + row['event_name'])
