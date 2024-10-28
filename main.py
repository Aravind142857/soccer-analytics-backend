import select
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text, select

from Analytics_backend.models import RawShotData
from .database import get_db, engine
from .crud import *
from statsbombpy import sb
import matplotlib.pyplot as plt
import matplotlib
from pydantic import BaseModel
from fastapi.responses import FileResponse
import pandas as pd
from mplsoccer import Pitch, VerticalPitch
import seaborn as sns
import json
import numpy as np

app = FastAPI()

MODELS_CHANGED = False
# def create_tables():
#     from .database import Base

#     Base.metadata.create_all(bind=engine)
#     print("Initialized the db")
if MODELS_CHANGED:
    with Session(engine) as session:
    # insert_shots_data(make_competition_df(2), session)
        match_df = sb.matches(competition_id =  2, season_id = 27)
        for match_id in match_df['match_id'].tolist():
            events = sb.events(match_id=match_id)
            insert_raw_shot_data(df=events[events['type'] == 'Shot'][['id', 'team','location','shot_end_location']], session=session)
            insert_raw_press_data(df=events[events['type'] == 'Pressure'][['id', 'team', 'location']], session = session)
            insert_team_passes(df=events[events['type'] == 'Pass'][['id', 'team', 'location', 'pass_end_location', 'pass_cross', 'pass_outcome']], session=session)
        insert_team_goals(df=match_df[['match_id', 'home_score', 'away_score', 'home_team', 'away_team']], session=session)


competitions: pd.DataFrame = pd.DataFrame(sb.competitions())

unique_competitions = competitions.competition_name.unique()
unique_seasons = competitions.season_name.unique()
competitions_grouped = competitions.groupby(['competition_name']).agg(
        season_name = ('season_name', list),
        competition_id = ('competition_id', 'first')
    )



# def make_competition_df(competition_id:int):
#     all_shots = []
#     matches = sb.matches(competition_id =  competition_id, season_id = 27)
#     match_ids = matches['match_id'].tolist()
#     for match_id in match_ids:
#         match = matches[matches['match_id'] == match_id]
#         home_team = match['home_team'].to_list()[0]
#         away_team = match['away_team'].to_list()[0]
#         match_events = sb.events(match_id=match_id)
#         shots = match_events[match_events['type'] == 'Shot']
#         home_shots = shots[shots['team'] == home_team][['location', 'shot_end_location']]
#         away_shots = shots[shots['team'] == away_team][['location', 'shot_end_location']]
#         all_shots.append({'team':home_team, 'shot_start':home_shots['location'].to_list(), 'shot_end':home_shots['shot_end_location'].to_list()})
#         all_shots.append({'team':away_team, 'shot_start':away_shots['location'].to_list(), 'shot_end':away_shots['shot_end_location'].to_list()})
#     shots_df = pd.DataFrame(all_shots)
#     shots_grouped_df = shots_df.groupby('team').agg(shot_start=('shot_start', lambda x: sum(x, [])), shot_end=('shot_end', lambda x: sum(x, []))).reset_index()
#     shots_grouped_df['num_shots'] = shots_grouped_df.apply(lambda x: len(x['shot_start']), axis=1)
#     return shots_grouped_df
        

# la_liga_df = make_competition_df(11)
# cl_df = make_competition_df(16)
# ligue_1_df = make_competition_df(7)
# bundesliga_df = make_competition_df(9)
# serie_a_df = make_competition_df(12)
@app.get("/api/data")
async def get_data():
    res = competitions.to_json(orient="records")
    parsed = json.loads(res)
    return parsed

@app.get("/api/get_stats/{team}")
async def get_stats(team, db: Session = Depends(get_db)):
    gf_row = get_team_goals_for(team, db)
    total_GF = int(gf_row.total_goals) if gf_row else 0
    gf_rank = int(gf_row.team_rank) if gf_row else 0

    ga_row = get_team_goals_against(team, db)
    total_GA = int(ga_row.total_goals) if ga_row else 0
    ga_rank = int(ga_row.team_rank) if ga_row else 0

    clean_sheets_row = get_team_clean_sheets(team, db)
    total_clean_sheets = int(clean_sheets_row.total_clean_sheets) if clean_sheets_row else 0
    clean_sheets_rank = int(clean_sheets_row.team_rank) if clean_sheets_row else 0

    shotRow = get_team_num_shots(team, db)
    total_shots = int(shotRow.shot_count) if shotRow else 0
    shot_rank = int(shotRow.team_rank) if shotRow else 0

    crossesRow = get_team_crosses_rank(team, db)
    total_crosses = int(crossesRow.cross_count) if crossesRow else 0
    cross_rank = int(crossesRow.team_rank) if crossesRow else 0

    passCompletionRow = get_pass_completion(team, db)
    pass_completion = round(float(passCompletionRow.completion_ratio), 2) if passCompletionRow else 0.0
    pass_completion_rank = int(passCompletionRow.team_rank) if passCompletionRow else 0

    passCompletionFinalThird = get_pass_completion_final_third(team, db)
    final_third_completion = round(float(passCompletionFinalThird.completion_ratio), 2) if passCompletionFinalThird else 0.0
    final_third_rank = int(passCompletionFinalThird.team_rank) if passCompletionFinalThird else 0
    return json.loads(json.dumps({
        "GF": total_GF,
        "GFR": gf_rank,
        "GA": total_GA,
        "GAR": ga_rank,
        "CS": total_clean_sheets,
        "CSR": clean_sheets_rank,
        "TS": total_shots,
        "TSR": shot_rank,
        "TC": total_crosses,
        "TCR": cross_rank,
        "PC": pass_completion,
        "PCR": pass_completion_rank,
        "FT": final_third_completion,
        "FTR": final_third_rank
        }))

@app.get("/api/get_teams")
async def get_teams(db: Session = Depends(get_db)):
    res = db.execute(select(RawShotData.team).distinct()).scalars().all()
    if res:
        return res
    else:
        return {"error": "No teams found"}
@app.get("/api/buttonLabels")
async def get_button_labels():
    res = competitions_grouped['season_name'].to_dict()
    return json.loads(json.dumps(res))
@app.get("/api/competitions15_16")
async def get_competitions_15_16():
    res = {"competitions":competitions[competitions.season_name == '2015/2016']['competition_name'].tolist()}
    return json.loads(json.dumps(res))
@app.get("/api/teams_from_competitions/{competition_name}")
async def get_teams_from_competitions(competition_name):
    if competition_name in unique_competitions:
        competition_df = competitions[competitions.competition_name == competition_name]
        competition_id = competition_df.competition_id.values[0]

        matches = json.loads(json.dumps({
            "teams":pd.DataFrame(sb.matches(competition_id=competition_id, season_id=27)).home_team.unique().tolist()
            }))
        return matches        
    else:
        return None

@app.get('/api/plot_team_shots_end/{team}')
def plot_team_shots_end(team: str, db: Session = Depends(get_db)):
    result = db.execute(text("SELECT team, json_agg(shot_end) AS all_shot_ends FROM raw_shot_data WHERE team = :team GROUP BY team;"), {'team': team}).fetchone()
    if result:
        matplotlib.use('Agg')
        result_dict = dict(result._mapping)
        shot_end = result_dict['all_shot_ends']
        shots_y = list(map(lambda y: y[0], shot_end))
        shots_x = list(map(lambda x: x[1], shot_end))
        pitch = VerticalPitch(goal_type='box', pitch_color='grass', line_color='white',
            axis=True, half=True, pad_left=-10, pad_right=-10, pad_bottom=-30)

        fig, ax = pitch.draw(figsize=(6,4))  # type: ignore
        for x, y in zip(shots_x, shots_y):
            color = 'red'
            ax.scatter(x, y, color=color, s=10, alpha=.4, zorder=2) if ax != None else None # type: ignore
        
        plt.title(f'Shot End Map of {team}')
        
        plt.savefig("Analytics_backend/tmp/plot.png")
        return FileResponse("Analytics_backend/tmp/plot.png")
    else:
        return {"error": "team does not exist in database."}

@app.get("/api/aggregate_shots/{team}")
def aggregate_shots(team: str, db: Session = Depends(get_db)):
    result = db.execute(text("SELECT team, COUNT(*) AS num_shots, json_agg(shot_start) AS all_shot_starts, json_agg(shot_end) AS all_shot_ends FROM raw_shot_data WHERE team = :team GROUP BY team;"), {'team': team}).fetchone()
    if result:
        result_dict = dict(result._mapping)
        return {
            'team': result_dict['team'],
            'num_shots': result_dict['num_shots'],
            'shot_start': result_dict['all_shot_starts'],
            'shot_end': result_dict['all_shot_ends']
        }
    else:
        return {"error": "team does not exist in database."}

@app.get("/api/team_shots/{team_name}")
def get_team_shots_endpoint(team_name:str, db:Session = Depends(get_db)):
    return json.loads(json.dumps(get_raw_shots(team_name, db), default=str))

@app.get("/api/plot_press_location/{team}")
def plot_press_location(team: str, db: Session = Depends(get_db)):
    result = db.execute(text("SELECT team, json_agg(press_location) AS all_press_locations FROM raw_press_data WHERE team = :team GROUP BY team;"), {'team': team}).fetchone()
    if result:
        matplotlib.use('Agg')
        result_dict = dict(result._mapping)
        press_locations = result_dict['all_press_locations']
        pitch = Pitch(pitch_type='statsbomb', pitch_color='grass', line_color='white', line_zorder=2)
        fig, ax = pitch.draw(figsize=(8, 6)) #type:ignore
        heatmap_data, x_edges, y_edges = np.histogram2d(
            list(map(lambda y: y[1], press_locations)), list(map(lambda x: x[0], press_locations)), bins=[4, 6], range=[[0, 80], [0, 120]])
        heatmap_density = (heatmap_data/heatmap_data.sum() * 100)
        sns.heatmap(heatmap_density, cmap='cividis', ax=ax, cbar=True, annot=True, fmt=".2f",alpha=0.8, linewidths=0.5, cbar_kws={"shrink": 0.5, "label":"Pressure Count"}, zorder=1) # type:ignore
        plt.title(f'Pressure Heatmap of {team}')
        plt.savefig("Analytics_backend/tmp/plot2.png")
        return FileResponse("Analytics_backend/tmp/plot2.png")
    else:
        return {"error": "team does not exist in database."}


@app.get("/api/plot_team_shots_start/{team}")
def plot_team_shots_start(team: str, db: Session = Depends(get_db)):
    result = db.execute(text("SELECT team, json_agg(shot_start) AS all_shot_starts FROM raw_shot_data WHERE team = :team GROUP BY team;"), {'team': team}).fetchone()
    if result:
        matplotlib.use('Agg')
        result_dict = dict(result._mapping)
        shot_start = result_dict['all_shot_starts']
        shots_y = list(map(lambda y: y[0], shot_start))
        shots_x = list(map(lambda x: x[1], shot_start))
        pitch = Pitch(pitch_type='statsbomb', pitch_color='grass', line_color='white', line_zorder=2)

        fig, ax = pitch.draw(figsize=(8,6))  # type: ignore
        heatmap_data, x_edges, y_edges = np.histogram2d(list(map(lambda y: y[1], shot_start)), list(map(lambda x: x[0], shot_start)), bins=[6,8], range=[[0,80], [0,120]])
        heatmap_density = (heatmap_data/heatmap_data.sum() * 100)
        sns.heatmap(heatmap_density, ax=ax, cmap='coolwarm', cbar=True, annot=True, fmt=".2f",alpha=.8, linewidths=.5, cbar_kws={"shrink": .5, "label":"Shot count"}) #type:ignore
        plt.title(f'Shot Heatmap of {team}', fontsize=16, fontweight='bold')
        # for x, y in zip(shots_x, shots_y):
        #     color = 'red'
        #     ax.scatter(x, y, color=color, s=10, alpha=.4, zorder=2) if ax != None else None # type: ignore
        
        # plt.title(f'Shot End Map of {team}')
        
        plt.savefig("Analytics_backend/tmp/plot1.png")
        return FileResponse("Analytics_backend/tmp/plot1.png")
    else:
        return {"error": "team does not exist in database."}


#     heatmap_data, x_edges, y_edges = np.histogram2d(list(map(lambda y: y[1], shot_start)), list(map(lambda x: x[0], shot_start)), bins=[6, 8], range=[[0, 80], [0, 120]])
#     heatmap_density = (heatmap_data/heatmap_data.sum())
#     sns.heatmap(heatmap_density, ax=ax, cmap='coolwarm', cbar=True, annot=True, fmt=".2f",alpha=.8, linewidths=.5, cbar_kws={"shrink": .5, "label":"Pressure Count"})
#     plt.title(f'Shot Heatmap of {team}', fontsize=16, fontweight='bold')
#     plt.show()