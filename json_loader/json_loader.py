import json
import psycopg2

conn = psycopg2.connect("host='localhost' dbname='Project' user='postgres' password='admin' port=5432")
curr = conn.cursor()

competitions_file = open('C:\\Users\\ethan\\Documents\\GitHub\\Project\\data\\competitions.json','r', encoding='utf8')
competition_data = json.load(competitions_file)

for competition in competition_data:
    if not ((competition['competition_name'] == "La Liga" and competition['season_name'] == "2020/2021") or (competition['competition_name'] == "La Liga" and competition['season_name'] == "2019/2020") or (competition['competition_name'] == "La Liga" and competition['season_name'] == "2018/2019") or (competition['competition_name'] == "Premier League" and competition['season_name'] == "2003/2004")):
        continue
    try:
        curr.execute(f"INSERT INTO competitions (competition_id, season_id, competition_name, competition_gender, country_name, season_name) VALUES ({competition['competition_id']},{competition['season_id']},'{competition['competition_name']}','{competition['competition_gender']}','{competition['country_name']}','{competition['season_name']}') ON CONFLICT DO NOTHING")
    except Exception as error:
        print(f"\nGiven input raised error: {error}")
    match_path = 'C:\\Users\\ethan\\Documents\\GitHub\\Project\\data\\matches\\' + str(competition['competition_id']) + '\\' + str(competition['season_id']) + '.json'
    match_file = open(match_path,'r', encoding='utf8')
    match_data = json.load(match_file)
    for match in match_data:
        try:
            stadium_id = match['stadium']['id']
            curr.execute(f"INSERT INTO countries (country_id, country_name) VALUES (%s, %s) ON CONFLICT DO NOTHING" ,(match['stadium']['country']['id'],match['stadium']['country']['name']))
            curr.execute(f"INSERT INTO stadiums (stadium_id, stadium_name, stadium_country_id) VALUES ({match['stadium']['id']},'{match['stadium']['name']}',{match['stadium']['country']['id']}) ON CONFLICT DO NOTHING")
        except:
            stadium_id = 'Null'
        
        try:
            referee_id = match['referee']['id']
            curr.execute(f"INSERT INTO referees (referee_id, referee_name, referee_country_id) VALUES ({match['referee']['id']},'{match['referee']['name']}',{match['referee']['country']['id']}) ON CONFLICT DO NOTHING")
        except:
            referee_id = 'Null'

        curr.execute(f"INSERT INTO countries (country_id, country_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",(match['home_team']['country']['id'],match['home_team']['country']['name']))
        curr.execute(f"INSERT INTO teams (team_id, team_name, team_gender, team_country_id) VALUES ({match['home_team']['home_team_id']},'{match['home_team']['home_team_name']}','{match['home_team']['home_team_gender']}',{match['home_team']['country']['id']}) ON CONFLICT DO NOTHING")
        curr.execute(f"INSERT INTO countries (country_id, country_name) VALUES (%s, %s) ON CONFLICT DO NOTHING", (match['away_team']['country']['id'],match['away_team']['country']['name']))
        curr.execute(f"INSERT INTO teams (team_id, team_name, team_gender, team_country_id) VALUES ({match['away_team']['away_team_id']},'{match['away_team']['away_team_name']}','{match['away_team']['away_team_gender']}',{match['away_team']['country']['id']}) ON CONFLICT DO NOTHING")
        try:
            for manager in match['home_team']['managers']:
                curr.execute(f"INSERT INTO countries (country_id, country_name) VALUES (%s, %s) ON CONFLICT DO NOTHING", (manager['country']['id'],manager['country']['name']))
                curr.execute(f"INSERT INTO managers (manager_id, manager_name, manager_nickname, manager_dob, manager_country_id) VALUES ({manager['id']},'{manager['name']}','{manager['nickname']}','{manager['dob']}',{manager['country']['id']}) ON CONFLICT DO NOTHING")
                curr.execute(f"INSERT INTO team_managers (team_id, manager_id) VALUES ({match['home_team']['home_team_id']},{manager['id']}) ON CONFLICT DO NOTHING")
        except:
            manager = None
        try:
            for manager in match['away_team']['managers']:
                curr.execute(f"INSERT INTO countries (country_id, country_name) VALUES (%s, %s) ON CONFLICT DO NOTHING", (manager['country']['id'],manager['country']['name']))
                curr.execute(f"INSERT INTO managers (manager_id, manager_name, manager_nickname, manager_dob, manager_country_id) VALUES ({manager['id']},'{manager['name']}','{manager['nickname']}','{manager['dob']}',{manager['country']['id']}) ON CONFLICT DO NOTHING")
                curr.execute(f"INSERT INTO team_managers (team_id, manager_id) VALUES ({match['away_team']['away_team_id']},{manager['id']}) ON CONFLICT DO NOTHING")
        except:
            manager = None

        curr.execute(f"INSERT INTO competition_stages (competition_stage_id, competition_stage_name) VALUES ({match['competition_stage']['id']},{match['competition_stage']['id']}) ON CONFLICT DO NOTHING")
        curr.execute(f"INSERT INTO matches (match_id, competition_id, season_id, match_date, kick_off, stadium_id, referee_id, home_team_id, away_team_id, home_score, away_score, match_week, competition_stage_id) VALUES ({match['match_id']},{competition['competition_id']},{competition['season_id']},'{match['match_date']}','{match['kick_off']}',{stadium_id},{referee_id},{match['home_team']['home_team_id']},{match['away_team']['away_team_id']},{match['home_score']},{match['away_score']},{match['match_week']},{match['competition_stage']['id']}) ON CONFLICT DO NOTHING")
        
        lineup_path = 'C:\\Users\\ethan\\Documents\\GitHub\\Project\\data\\lineups\\' + str(match["match_id"]) + '.json'
        lineup_file = open(lineup_path, 'r', encoding='utf8')
        lineup_data = json.load(lineup_file)

        
        for lineup in lineup_data:
            for player in lineup['lineup']:
                curr.execute(f"""INSERT INTO countries (country_id, country_name) VALUES (%s, %s) ON CONFLICT DO NOTHING""",(player['country']['id'],player['country']['name']))
                curr.execute(f"INSERT INTO players (player_id, player_name, player_nickname, player_country_id) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", (player['player_id'],player['player_name'],player['player_nickname'],player['country']['id']))
                curr.execute(f"INSERT INTO lineups (match_id, team_id, player_id, jersey_number) VALUES ({match['match_id']},{lineup['team_id']},{player['player_id']},{player['jersey_number']}) ON CONFLICT DO NOTHING")
            
        event_path = 'C:\\Users\\ethan\\Documents\\GitHub\\Project\\data\\events\\' + str(match["match_id"]) + '.json'
        event_file = open(event_path, 'r', encoding='utf8')
        event_data = json.load(event_file)

        for event in event_data:
            curr.execute(f"INSERT INTO events (event_id,match_id,competition_id,season_id,event_index,event_period,event_timestamp,event_minute,event_second,type_id,type_name,possession,possession_team_id,possession_team_name,play_pattern_id,play_pattern_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],match['match_id'],competition['competition_id'],competition['season_id'],event['index'],event['period'],event['timestamp'],event['minute'],event['second'],event['type']['id'],event['type']['name'],event['possession'],event['possession_team']['id'],event['possession_team']['name'],event['play_pattern']['id'],event['play_pattern']['name']))
            if event['type']['id'] == 33:       ### 50/50
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False
                try:
                    counterpress = event['counterpress']
                except:
                    counterpress = False

                curr.execute(f"INSERT INTO fiftyfifty (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,out_of_bounds,outcome_id, outcome_name,counterpress) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING" ,(event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,out,event['50_50']['outcome']['id'],event['50_50']['outcome']['name'],counterpress))
            elif event['type']['id'] == 24:     ## Bad Behaviour
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False

                curr.execute(f"INSERT INTO bad_behaviour (event_id,team_id,team_name,player_id,player_name,position_id,position_name,duration,under_pressure,off_camera,bad_behaviour_card_id,bad_behaviour_card_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['duration'],under_pressure,off_camera,event['bad_behaviour']['card']['id'],event['bad_behaviour']['card']['name']))
            elif event['type']['id'] == 42:   ### Ball Receipt*
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False

                curr.execute(f"INSERT INTO ball_receipt (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure))
            elif event['type']['id'] == 2:  ### Ball Recovery
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False
                try:
                    recovery_failure = event['ball_recovery']['recovery_failure']
                except:
                    recovery_failure = False
                try:
                    offensive = event['ball_recovery']['offensive']
                except:
                    offensive = False

                curr.execute(f"INSERT INTO ball_recovery (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,duration,under_pressure,off_camera,out_of_bounds,recovery_failure,offensive) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],event['duration'],under_pressure,off_camera,out,recovery_failure,offensive))
            elif event['type']['id'] == 6:  ### Block
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False
                try:
                    counterpress = event['counterpress']
                except:
                    counterpress = False
                try:
                    deflection = event['block']['deflection']
                except:
                    deflection = False
                try:
                    offensive = event['block']['offensive']
                except:
                    offensive = False
                try:
                    save_block = event['block']['save_block']
                except:
                    save_block = False

                curr.execute(f"INSERT INTO block (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,out_of_bounds,deflection,offensive,save_block,counterpress) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,out,deflection,offensive,save_block,counterpress))
            elif event['type']['id'] == 5:   ### Camera On
                curr.execute(f"INSERT INTO camera_on (event_id,team_id,team_name) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}') ON CONFLICT DO NOTHING")
            elif event['type']['id'] == 43:  ### Carry
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False

                curr.execute(f"INSERT INTO carry (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,duration,under_pressure) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],event['duration'],under_pressure))
            elif event['type']['id'] == 9:   ### Clearance
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False
                try:
                    aerial_won = event['clearance']['aerial_won']
                except:
                    aerial_won = False

                curr.execute(f"INSERT INTO clearance (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,out_of_bounds,aerial_won) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,out,aerial_won))
            elif event['type']['id'] == 3:  ### Dispossessed
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False

                curr.execute(f"INSERT INTO dispossessed (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera))
            elif event['type']['id'] == 14: ### Dribble
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False
                try:
                    nutmeg = event['dribble']['nutmeg']
                except:
                    nutmeg = False
                try:
                    overrun = event['dribble']['overrun']
                except:
                    overrun = False
                try:
                    no_touch = event['dribble']['no_touch']
                except:
                    no_touch = False

                curr.execute(f"INSERT INTO dribble (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,out_of_bounds,outcome_id, outcome_name,nutmeg,overrun,no_touch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,out,event['dribble']['outcome']['id'],event['dribble']['outcome']['name'],nutmeg,overrun,no_touch))
            elif event['type']['id'] == 39: ###Dribbled Past    
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
            
                try:
                    counterpress = event['counterpress']
                except:
                    counterpress = False

                curr.execute(f"INSERT INTO dribbled_past (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,off_camera,counterpress) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],off_camera,counterpress))
            elif event['type']['id'] == 4:  ### Duel
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    counterpress = event['counterpress']
                except:
                    counterpress = False

                try:
                    outcome_id = event['duel']['outcome']['id']
                    outcome_name = event['duel']['outcome']['name']
                except:
                    outcome_id = None
                    outcome_name = None

                curr.execute(f"INSERT INTO duel (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,counterpress,duel_type_id,duel_type_name,outcome_id,outcome_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,counterpress,event['duel']['type']['id'],event['duel']['type']['name'],outcome_id,outcome_name))
            
            elif event['type']['id'] == 37: ### Error

                curr.execute(f"INSERT INTO error (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['player']['id']},'{event['player']['name']}',{event['position']['id']},'{event['position']['name']}',{event['location'][0]},{event['location'][1]}) ON CONFLICT DO NOTHING")
                          
            elif event['type']['id'] == 22: ### Foul Commited
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False

                try:
                    counterpress = event['counterpress']
                except:
                    counterpress = False

                try:
                    advantage = event['foul_commited']['advantage']
                except:
                    advantage = False
                try:
                    card_id = event['foul_commited']['card']['id']
                    card_name = event['foul_commited']['card']['name']
                except:
                    card_id = None
                    card_name = None
                try:
                    penalty = event['foul_commited']['penalty']
                except:
                    penalty = False
                try:
                    offensive = event['foul_commited']['offensive']
                except:
                    offensive = False

                curr.execute(f"INSERT INTO foul_commited (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,counterpress,advantage,card_id,card_name,penalty,offensive) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,counterpress,advantage,card_id,card_name,penalty,offensive))
            
            elif event['type']['id'] == 21: ### Foul Won
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False

                try:
                    advantage = event['foul_commited']['advantage']
                except:
                    advantage = False
                try:
                    penalty = event['foul_won']['penalty']
                except:
                    penalty = False
                try:
                    defensive = event['foul_won']['defensive']
                except:
                    defensive = False

                curr.execute(f"INSERT INTO foul_won (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,event_defensive,advantage,penalty) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,defensive,advantage,penalty))
                            
            elif event['type']['id'] == 23: ### Goal Keeper
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False
                try:
                    outcome_id = event['goalkeeper']['outcome']['id']
                    outcome_name = event['goalkeeper']['outcome']['name']
                except:
                    outcome_id = 'Null'
                    outcome_name = 'Null'

                try:
                    technique_id = event['goalkeeper']['technique']['id']
                    technique_name = event['goalkeeper']['technique']['name']
                except:
                    technique_id = 'Null'
                    technique_name = 'Null'

                try:
                    goalkeeper_position_id = event['goalkeeper']['position']['id']
                    goalkeeper_position_name = event['goalkeeper']['position']['name']
                except:
                    goalkeeper_position_id = 'Null'
                    goalkeeper_position_name = 'Null'

                try:
                    location_x = event['location'][0]
                    location_y = event['location'][1]
                except:
                    location_x = 'Null'
                    location_y = 'Null'

                curr.execute(f"INSERT INTO goalkeeper (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,out_of_bounds,technique_id,technique_name,save_position_id,save_position_name,save_type_id,save_type_name,outcome_id,outcome_name) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['player']['id']},'{event['player']['name']}',{event['position']['id']},'{event['position']['name']}',{location_x},{location_y},{under_pressure},{off_camera},{out},{technique_id},'{technique_name}',{goalkeeper_position_id},'{goalkeeper_position_name}',{event['goalkeeper']['type']['id']},'{event['goalkeeper']['type']['name']}',{outcome_id},'{outcome_name}') ON CONFLICT DO NOTHING")
            
            elif event['type']['id'] == 34:     ### Half End

                try:
                    early_video_end = event['half_end']['early_video_end']
                except:
                    early_video_end = False
                
                try:
                    match_suspended = event['half_end']['match_suspended']
                except:
                    match_suspended = False

                curr.execute(f"INSERT INTO half_end (event_id,team_id,team_name,early_video_end,match_suspended) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{early_video_end},{match_suspended}) ON CONFLICT DO NOTHING")
            
            elif event['type']['id'] == 18:     ### Half Start

                try:
                    late_video_start = event['late_video_start']
                except:
                    late_video_start = False

                curr.execute(f"INSERT INTO half_start (event_id,team_id,team_name,late_video_start) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}','{late_video_start}') ON CONFLICT DO NOTHING")
            elif event['type']['id'] == 40:     ### Injury Stoppage
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    in_chain = event['injury_stoppage']['in_chain']
                except:
                    in_chain = False

                curr.execute(f"INSERT INTO injury_stoppage (event_id,team_id,team_name,under_pressure,off_camera,in_chain) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{under_pressure},{off_camera},{in_chain}) ON CONFLICT DO NOTHING")
            
            elif event['type']['id'] == 10:     ### Interception
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    counterpress = event['counterpress']
                except:
                    counterpress = False

                curr.execute(f"INSERT INTO interception (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,counterpress,outcome_id,outcome_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,counterpress,event['interception']['outcome']['id'],event['interception']['outcome']['name']))
            
            elif event['type']['id'] == 38:     ### Miscontrol
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False
                try:
                    aerial_won = event['aerial_won']
                except:
                    aerial_won = False

                curr.execute(f"INSERT INTO miscontrol (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,out_of_bounds,aerial_won) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,out,aerial_won))
            
            elif event['type']['id'] == 8:      ### Offside

                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False

                curr.execute(f"INSERT INTO offside (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,out_of_bounds) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['player']['id']},'{event['player']['name']}',{event['position']['id']},'{event['position']['name']}',{event['location'][0]},{event['location'][1]},{under_pressure},{off_camera},{out}) ON CONFLICT DO NOTHING")

            elif event['type']['id'] == 20:     ### Own Goal Against
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False

                curr.execute(f"INSERT INTO own_goal_against (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,duration,under_pressure,off_camera,out_of_bounds) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['player']['id']},'{event['player']['name']}',{event['position']['id']},'{event['position']['name']}',{event['location'][0]},{event['location'][1]},{event['duration']},{under_pressure},{off_camera},{out}) ON CONFLICT DO NOTHING")
            
            elif event['type']['id'] == 25:    ### Own Goal For
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False

                curr.execute(f"INSERT INTO own_goal_for (event_id,team_id,team_name,location_x,location_y,duration,under_pressure,off_camera,out_of_bounds) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['location'][0]},{event['location'][1]},{event['duration']},{under_pressure},{off_camera},{out}) ON CONFLICT DO NOTHING")
            
            elif event['type']['id'] == 30:    ### Pass
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False

                try:
                    outcome_id = event['pass']['outcome']['id']
                    outcome_name = event['pass']['outcome']['name']
                except:
                    outcome_id = None
                    outcome_name = None
                
                try:
                    technique_id = event['pass']['technique']['id']
                    technique_name = event['pass']['technique']['name']
                except:
                    technique_id = None
                    technique_name = None
                
                try:
                    shot_assist = event['pass']['shot_assist']
                except:
                    shot_assist = False

                try:
                    goal_assist = event['pass']['goal_assist']
                except:
                    goal_assist = False

                try:
                    switch = event['pass']['switch']
                except:
                    switch = False

                try:
                    cut_back = event['pass']['cut_back']
                except:
                    cut_back = False

                try:
                    cross = event['pass']['cross']
                except:
                    cross = False

                try:
                    miscommunication = event['pass']['miscommunication']
                except:
                    miscommunication = False

                try:
                    backheel = event['pass']['backheel']
                except:
                    backheel = False

                try:
                    deflection = event['pass']['deflection']
                except:
                    deflection = False

                try:
                    aerial_won = event['pass']['aerial_won']
                except:
                    aerial_won = False

                try:
                    type_id = event['pass']['type']['id']
                    type_name = event['pass']['type']['name']
                except:
                    type_id = None
                    type_name = None

                try:
                    body_part_id = event['pass']['body_part']['id']
                    body_part_name = event['pass']['body_part']['name']
                except:
                    body_part_id = None
                    body_part_name = None

                try:
                    recipient_id = event['pass']['recipient']['id']
                    recipient_name = event['pass']['recipient']['name']
                except:
                    recipient_id = None
                    recipient_name = None

                curr.execute(f"INSERT INTO pass (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,duration,under_pressure,off_camera,out_of_bounds,pass_recipient_id,pass_recipient_name,pass_length,pass_angle,height_id,height_name,end_location_x,end_location_y,body_part_id,body_part_name,pass_type_id,pass_type_name,outcome_id,outcome_name,technique_id,technique_name,shot_assist,goal_assist,switch,cut_back,crossed,miscommunication,backheel,deflected,aerial_won) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],event['duration'],under_pressure,off_camera,out,recipient_id,recipient_name,event['pass']['length'],event['pass']['angle'],event['pass']['height']['id'],event['pass']['height']['name'],event['pass']['end_location'][0],event['pass']['end_location'][1],body_part_id,body_part_name,type_id,type_name,outcome_id,outcome_name,technique_id,technique_name,shot_assist,goal_assist,switch,cut_back,cross,miscommunication,backheel,deflection,aerial_won))
            
            elif event['type']['id'] == 27:    ### Player Off
                try:
                    permanent = event['permanent']
                except:
                    permanent = False

                curr.execute(f"INSERT INTO player_off (event_id,team_id,team_name,player_id,player_name,position_id,position_name,permanent) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['player']['id']},'{event['player']['name']}',{event['position']['id']},'{event['position']['name']}',{permanent}) ON CONFLICT DO NOTHING")
            
            elif event['type']['id'] == 26:     ### Player On

                curr.execute(f"INSERT INTO player_on (event_id,team_id,team_name,player_id,player_name,position_id,position_name) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['player']['id']},'{event['player']['name']}',{event['position']['id']},'{event['position']['name']}') ON CONFLICT DO NOTHING")
            
            elif event['type']['id'] == 17:    ### Pressure

                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False

                try:
                    counterpress = event['counterpress']
                except:
                    counterpress = False

                curr.execute(f"INSERT INTO pressure (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,duration,under_pressure,off_camera,counterpress) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],event['duration'],under_pressure,off_camera,counterpress))
            
            elif event['type']['id'] == 41:     ### Referee Ball-Drop

                curr.execute(f"INSERT INTO referee_ball_drop (event_id,team_id,team_name,location_x,location_y) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['location'][0]},{event['location'][1]}) ON CONFLICT DO NOTHING")

            elif event['type']['id'] == 28:    ### Shield
                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False

                curr.execute(f"INSERT INTO shield (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,under_pressure,off_camera,out_of_bounds) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],under_pressure,off_camera,out))

            elif event['type']['id'] == 16:     ### Shot

                try:
                    under_pressure = event['under_pressure']
                except:
                    under_pressure = False
                
                try: 
                    off_camera = event['off_camera']
                except:
                    off_camera = False
                
                try:
                    out = event['out']
                except:
                    out = False

                try:
                    aerial_won = event['shot']['aerial_won']
                except:
                    aerial_won = False

                try:
                    follows_dribble = event['shot']['follows_dribble']
                except:
                    follows_dribble = False
                
                try:
                    first_time = event['shot']['first_time']
                except:
                    first_time = False
                
                try:
                    open_goal = event['shot']['open_goal']
                except:
                    open_goal = False

                try:
                    deflected = event['shot']['deflected']
                except:
                    deflected = False

                try:
                    end_location_z = event['shot']['end_location'][2]
                except:
                    end_location_z = None

                curr.execute(f"INSERT INTO shot (event_id,team_id,team_name,player_id,player_name,position_id,position_name,location_x,location_y,duration,under_pressure,off_camera,out_of_bounds,end_location_x,end_location_y,end_location_z,statsbomb_xg,outcome_id,outcome_name,technique_id,technique_name,body_part_id,body_part_name,shot_type_id,shot_type_name,aerial_won,follows_dribble,first_time,open_goal,deflected) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", (event['id'],event['team']['id'],event['team']['name'],event['player']['id'],event['player']['name'],event['position']['id'],event['position']['name'],event['location'][0],event['location'][1],event['duration'],under_pressure,off_camera,out,event['shot']['end_location'][0],event['shot']['end_location'][1],end_location_z,event['shot']['statsbomb_xg'],event['shot']['outcome']['id'],event['shot']['outcome']['name'],event['shot']['technique']['id'],event['shot']['technique']['name'],event['shot']['body_part']['id'],event['shot']['body_part']['name'],event['shot']['type']['id'],event['shot']['type']['name'],aerial_won,follows_dribble,first_time,open_goal,deflected))
            
                
            elif event['type']['id'] == 35:     ### Starting XI
                
                curr.execute(f"INSERT INTO starting_xi (event_id,team_id,team_name,formation) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['tactics']['formation']}) ON CONFLICT DO NOTHING")

            elif event['type']['id'] == 19:     ### Substitution

                curr.execute(f"INSERT INTO substitution (event_id,team_id,team_name,player_id,player_name,position_id,position_name,duration,outcome_id,outcome_name,replacement_id,replacement_name) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['player']['id']},'{event['player']['name']}',{event['position']['id']},'{event['position']['name']}',{event['duration']},{event['substitution']['outcome']['id']},'{event['substitution']['outcome']['name']}',{event['substitution']['replacement']['id']},'{event['substitution']['replacement']['name']}') ON CONFLICT DO NOTHING")
            
            elif event['type']['id'] == 36:     ### Tactical Shift

                curr.execute(f"INSERT INTO tactical_shift (event_id,team_id,team_name,formation) VALUES ('{event['id']}',{event['team']['id']},'{event['team']['name']}',{event['tactics']['formation']}) ON CONFLICT DO NOTHING")
            
            else:
                print(f"{str(event['type']['id'])} + {str(event['type']['name'])}")
        conn.commit()
    conn.commit()
curr.close()
            
            

