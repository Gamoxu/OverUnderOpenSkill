#!/usr/bin/env python
# coding: utf-8

# Adapted from: 
#     https://github.com/owsorber/FTC_OPR_Calculator/blob/master/OPR.py
"""
FTC Team Statistics Calculator
@author: owsorber/dcieslak
OPR = Offensive Power Rating
CCWM = Calculated Contribution to Winning Margin
"""

import numpy as np # has matrix calculations
import pandas as pd
import trueskill as ts
import math
import itertools

BETA=4.166666666666667

class Alliance:
    def __init__(self, team1, team2, score, col):
        self.team1 = team1
        self.team2 = team2
        self.score = score
        self.col = col
    
    def __str__(self):
        return self.col + " Alliance: " + str(self.team1) + ", " + (self.team2)

class Match:
    def __init__(self, num, redAlliance, blueAlliance):
        self.num = num
        self.redAlliance = redAlliance
        self.blueAlliance = blueAlliance
    
    def __str__(self):
        return "Match # " + str(self.num) + ": (" + str(self.redAlliance.team1.num) + " & " + str(self.redAlliance.team2.num) + ") " + str(self.redAlliance.score) + " - " + str(self.blueAlliance.score) + " (" + str(self.blueAlliance.team1.num) + " & " +  str(self.blueAlliance.team2.num) + ")"


def _load_matches(df_matches):
    """
    Loads the match data. Assumes dataframe has:
    red_team1_id, red_team2_id, red_score, blue_team1_id, blue_team2_id, blue_score
    """
    matches = []
        
    matchNum = 1
    for index, row in df_matches.iterrows():
        redAlliance = Alliance(row['red_team1_id'], row['red_team2_id'], row['red_score'], "Red")
        blueAlliance = Alliance(row['blue_team1_id'], row['blue_team2_id'], row['blue_score'], "Blue")
        
        matches.append(Match(matchNum, redAlliance, blueAlliance))
        matchNum += 1
    
    return matches

def _load_teams(df_teams):
    """ 
    Loads the match data. Assumes dataframe has:
    id, number
    Where id is an integer and number is like 355V
    """
    teams = {}
    
    for index, row in df_teams.iterrows():
        teams[int(row['id'])] = row['number']
    
    return teams

def _build_match_matrix(matches, teams):
    """ 
    Build M, a matrix of alliances x teams, where each row indicates the teams in that alliance.
    A value of 1 means the team was in that alliance and a value of 0 means the team was not.
    First loop through each red alliance and then loop through each blue alliance.
    The resulting matrix should have 2 * len(matches) rows.
    """
    M = []
    for match in matches:
        r = []
        for team in teams:
            if match.redAlliance.team1 == team or match.redAlliance.team2 == team:
                r.append(1)
            else:
                r.append(0)
        M.append(r)

        b = []
        for team in teams:
            if match.blueAlliance.team1 == team or match.blueAlliance.team2 == team:
                b.append(1)
            else:
                b.append(0)
        M.append(b)
    return np.matrix(M)

def _build_match_scores_matrix(matches):
    """
    Build Scores, a matrix of alliances x 1, where each row indicates the score of that alliance.
    The alliance represented by each row corresponds to the alliance represented by each row
    in the matrix M.
    """
    Scores = []
    for match in matches:
        Scores.append([match.redAlliance.score])
        Scores.append([match.blueAlliance.score])
    return np.matrix(Scores)

def _build_match_margins_matrix(matches):
    """
    Build Margins, a matrix of alliances x 1, where each row indicates the margin of victory/loss 
    of that alliance (e.g. if an alliance wins 60-50, the value is +10).
    The alliance represented by each row corresponds to the alliance represented by each row
    in the matrix M.
    """
    Margins = []
    for match in matches:
        Margins.append([match.redAlliance.score - match.blueAlliance.score])
        Margins.append([match.blueAlliance.score - match.redAlliance.score])
    return np.matrix(Margins)

def _compute_pseudoinverse(M, S):
    """ 
    Find the pseudoinverse of the matrix M. Multiplying this by a results matrix will find the
    solution to the overdetermined system of equations.
    """
    pseudoinverse = np.linalg.pinv(M)
    return np.matmul(pseudoinverse, S)

def calculate_opr_ccwm(df_matches, df_teams):
    """ 
    Loads the match data. Assumes dataframe df_matches has:
        red_team1_id, red_team2_id, red_score, blue_team1_id, blue_team2_id, blue_score
    and df_teams has:
        id, number
    """
    teams = _load_teams(df_teams)
    matches = _load_matches(df_matches)
    M = _build_match_matrix(matches, teams)
    Scores = _build_match_scores_matrix(matches)
    Margins = _build_match_margins_matrix(matches)
    OPRs = _compute_pseudoinverse(M, Scores)
    CCWMs = _compute_pseudoinverse(M, Margins)
    listOPR = convertToList(OPRs)
    listCCWM = convertToList(CCWMs)
    df_retval = None
    i = 0
    for team in teams:
        d = {'team_id':team,'opr':listOPR[i],'ccwm':listCCWM[i],'dpr':(listOPR[i]-listCCWM[i])}
        df_row = pd.DataFrame([d])
        i = i + 1
        if df_retval is None:
            df_retval = df_row
        else:
            df_retval = df_retval.append(df_row)
    return df_retval

# Converts any stat represented by a matrix into a list, used later for sorting
def convertToList(statMatrix):
    l = []
    for val in statMatrix:
        l.append(round(float(val), 3))
    return l

def calculate_trueskill(df_matches, df_teams):
    rt2 = df_matches.red_team2_code.unique()
    rt1 = df_matches.red_team1_code.unique()
    bt1 = df_matches.blue_team1_code.unique()
    bt2 = df_matches.blue_team2_code.unique()

    tts = dict()

    for t in np.unique(np.concatenate((rt1,rt2,bt1,bt2))):
        tts[t] = ts.Rating()

    for index, row in df_matches.iterrows():
        r1 = row['red_team1_code']
        r2 = row['red_team2_code']
        b1 = row['blue_team1_code']
        b2 = row['blue_team2_code']

        if row['red_score'] > row['blue_score']:
            (tts[r1],tts[r2]), (tts[b1],tts[b2]) = ts.rate([(tts[r1],tts[r2]), (tts[b1],tts[b2])], ranks=[0, 1])     
        elif row['red_score'] > row['blue_score']:
            (tts[r1],tts[r2]), (tts[b1],tts[b2]) = ts.rate([(tts[r1],tts[r2]), (tts[b1],tts[b2])], ranks=[1, 0])     
        else:
            (tts[r1],tts[r2]), (tts[b1],tts[b2]) = ts.rate([(tts[r1],tts[r2]), (tts[b1],tts[b2])], ranks=[0, 0])     
        
    df_retval = None
    for index, row in df_teams.iterrows():
        try:
            d = {'team_code':row['number'],'trueskill':tts[row['number']].mu-3*tts[row['number']].sigma,
                 'mu':tts[row['number']].mu,'ts_sigma':tts[row['number']].sigma}
        except:
            tts[row['number']] = ts.Rating()
            d = {'team_code':row['number'],'trueskill':tts[row['number']].mu-3*tts[row['number']].sigma,
                 'mu':tts[row['number']].mu,'ts_sigma':tts[row['number']].sigma}
        df_row = pd.DataFrame([d])
        if df_retval is None:
            df_retval = df_row
        else:
            df_retval = df_retval.append(df_row)
    return df_retval, tts

def trueskill_win_probability(team1, team2):
    delta_mu = sum(r.mu for r in team1) - sum(r.mu for r in team2)
    sum_sigma = sum(r.sigma ** 2 for r in itertools.chain(team1, team2))
    size = len(team1) + len(team2)
    denom = math.sqrt(size * (BETA * BETA) + sum_sigma)
    ts_ge = ts.global_env()
    return ts_ge.cdf(delta_mu / denom)

def calculate_schedule_strength(df_matches_by_team, df_rankings):
    """ Calculates Schedule Strength for an event
    
    Thought about variations on:
        SS = (Opp1.Wins + Opp2.Wins - 2 * My.Losses) - (Partner.Wins - My.Wins)
        SS = (Opp1.Wins + Opp2.Wins) - (Partner.Wins - My.Wins)
        SS = (Opp1.Wins + Opp2.Wins) - Partner.Wins
        SS = Partner.Wins - My.Wins
        SS = Opp1.Wins + Opp2.Wins

    Settled on:
        SS = Opp1.Wins + Opp2.Wins

    Assumes `df_matches_by_team` is the output of event_matches_pivot_to_team_matches
    and `df_rankings` is the event's rankings
    """
    df_retval = None
    i = 0
    for index, row in df_rankings.iterrows():
        #partner_teams = df_matches_by_team.query("team_code=='" + row['team_name'] + "'")['partner_code'].unique()
        #df_partners = pd.DataFrame(partner_teams, columns=['team_code'])
        df_partners = df_matches_by_team.query("team_code=='" + row['team_name'] + "'")[['partner_code']].rename(columns={'partner_code':'team_code'})

        opp1_teams = df_matches_by_team.query("team_code=='" + row['team_name'] + "'")['opp1_code'].unique()
        opp2_teams = df_matches_by_team.query("team_code=='" + row['team_name'] + "'")['opp2_code'].unique()
        opp_teams = np.append(opp1_teams,opp2_teams)
        # opp_teams = np.unique(opp_teams)

        df_opp = pd.DataFrame(opp_teams, columns=['team_code'])

        p_wins = df_partners.join(df_rankings[['team_name','wins']].set_index("team_name"),on='team_code',how='left')['wins'].sum()
        p_ap = df_partners.join(df_rankings[['team_name','ap']].set_index("team_name"),on='team_code',how='left')['ap'].sum()
        o_wins = df_opp.join(df_rankings[['team_name','wins']].set_index("team_name"),on='team_code',how='left')['wins'].sum()
        o_ap = df_opp.join(df_rankings[['team_name','ap']].set_index("team_name"),on='team_code',how='left')['ap'].sum()

        m_wins = df_rankings[['team_name','wins']].query("team_name=='" + row['team_name'] + "'")['wins'].sum()
        m_losses = df_rankings[['team_name','losses']].query("team_name=='" + row['team_name'] + "'")['losses'].sum()
        # ss = ((o_wins - 2 * m_losses) - (p_wins - m_wins))
        # ss = o_wins - 2 * m_losses
        # ss = o_wins - (p_wins - m_wins)
        # ss = p_wins - m_wins
        ss = o_wins
        ssap = o_ap - p_ap
        d = {'team_code':row['team_name'],'p_wins':p_wins,'o_wins':o_wins,'ss':ss,'ssap':ssap}
        df_row = pd.DataFrame([d])
        i = i + 1
        if df_retval is None:
            df_retval = df_row
        else:
            df_retval = df_retval.append(df_row)
    return df_retval
