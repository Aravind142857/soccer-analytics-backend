from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from .models import PassData, RawPressData, Shots, RawShotData, MatchData
from .database import engine
import pandas as pd
from sqlalchemy import Cast, Float, func, desc, cast, asc



def insert_shots_data(df:pd.DataFrame, db:Session):
    for _, row in df.iterrows():
        shot = Shots(
            team=row['team'],
            shot_start=row['shot_start'],
            shot_end=row['shot_end'],
            num_shots=row['num_shots'],
        )
        db.add(shot)
    db.commit()

def get_team_shots(team_name:str, db:Session):
    return db.query(Shots).filter(Shots.team == team_name).all()

def insert_raw_shot_data(df:pd.DataFrame, session:Session):
    for idx, row in df.iterrows():
        shot = insert(RawShotData).values(
            id = row['id'],
            team = row['team'],
            shot_start = row['location'],
            shot_end = row['shot_end_location'],
        ).on_conflict_do_nothing()
        session.execute(shot)
    session.commit()

def get_raw_shots(team_name:str, session:Session):
    shots = session.query(RawShotData).filter(RawShotData.team == team_name).limit(5).all()
    return [shot.to_dict() for shot in shots]

def insert_raw_press_data(df: pd.DataFrame, session: Session):
    for idx, row in df.iterrows():
        press = insert(RawPressData).values(
            id = row['id'],
            team = row['team'],
            press_location = row['location']
        ).on_conflict_do_nothing()
        session.execute(press)
    session.commit()

def get_raw_presses(team: str, session: Session):
    presses = session.query(RawPressData).filter(RawPressData.team == team).limit(5).all()
    return [press.to_dict() for press in presses]

def get_team_goals_for(team: str, session: Session):
    # goals = session.query(func.sum(MatchData.home_goals)).filter(MatchData.home_team == team).scalar() or 0
    # goals += session.query(func.sum(MatchData.away_goals)).filter(MatchData.away_team == team).scalar() or 0
    home_goals = session.query(MatchData.home_team.label("team"), func.sum(MatchData.home_goals).label("goals")).group_by(MatchData.home_team).subquery()
    away_goals = session.query(MatchData.away_team.label("team"), func.sum(MatchData.away_goals).label("goals")).group_by(MatchData.away_team).subquery()
    combined_goals = session.query(home_goals.c.team, func.sum(home_goals.c.goals + func.coalesce(away_goals.c.goals, 0)).label("total_goals")).join(away_goals, home_goals.c.team == away_goals.c.team, isouter=True).group_by(home_goals.c.team).subquery()
    ranked_goals = session.query(combined_goals.c.team, combined_goals.c.total_goals, func.rank().over(order_by=combined_goals.c.total_goals.desc()).label("team_rank")).subquery()
    team_goals_row = session.query(ranked_goals.c.team_rank, ranked_goals.c.total_goals).filter(ranked_goals.c.team == team).first()
    return team_goals_row

def get_team_goals_against(team: str, session: Session):
    # goals = session.query(func.sum(MatchData.home_goals)).filter(MatchData.away_team == team).scalar() or 0
    # goals += session.query(func.sum(MatchData.away_goals)).filter(MatchData.home_team == team).scalar() or 0
    home_goals = session.query(MatchData.home_team.label("team"), func.sum(MatchData.away_goals).label("goals")).group_by(MatchData.home_team).subquery()
    away_goals = session.query(MatchData.away_team.label("team"), func.sum(MatchData.home_goals).label("goals")).group_by(MatchData.away_team).subquery()
    combined_goals = session.query(home_goals.c.team, func.sum(home_goals.c.goals + func.coalesce(away_goals.c.goals, 0)).label("total_goals")).join(away_goals, home_goals.c.team == away_goals.c.team, isouter=True).group_by(home_goals.c.team).subquery()
    ranked_goals = session.query(combined_goals.c.team, combined_goals.c.total_goals, func.rank().over(order_by=combined_goals.c.total_goals.asc()).label("team_rank")).subquery()
    team_goals_row = session.query(ranked_goals.c.team_rank, ranked_goals.c.total_goals).filter(ranked_goals.c.team == team).first()
    return team_goals_row

def get_team_clean_sheets(team: str, session: Session):
    # clean_sheets = session.query(func.count(MatchData.id)).filter(MatchData.home_team == team, MatchData.away_goals == 0).scalar() or 0
    # clean_sheets += session.query(func.count(MatchData.id)).filter(MatchData.away_team == team, MatchData.home_goals == 0).scalar() or 0
    home_clean_sheets = session.query(MatchData.home_team.label("team"), func.count(MatchData.id).label("clean_sheets")).filter(MatchData.away_goals == 0).group_by(MatchData.home_team).subquery()
    away_clean_sheets = session.query(MatchData.away_team.label("team"), func.count(MatchData.id).label("clean_sheets")).filter(MatchData.home_goals == 0).group_by(MatchData.away_team).subquery()
    combined_clean_sheets = session.query(home_clean_sheets.c.team, func.sum(home_clean_sheets.c.clean_sheets + func.coalesce(away_clean_sheets.c.clean_sheets, 0)).label("total_clean_sheets")).join(away_clean_sheets, home_clean_sheets.c.team == away_clean_sheets.c.team, isouter=True).group_by(home_clean_sheets.c.team).subquery()
    ranked_clean_sheets = session.query(combined_clean_sheets.c.team, combined_clean_sheets.c.total_clean_sheets, func.rank().over(order_by=combined_clean_sheets.c.total_clean_sheets.desc()).label("team_rank")).subquery()
    team_clean_sheets_row = session.query(ranked_clean_sheets.c.team_rank, ranked_clean_sheets.c.total_clean_sheets).filter(ranked_clean_sheets.c.team == team).first()
    return team_clean_sheets_row

def get_team_num_shots(team: str, session: Session):
    # shots = session.query(func.count(RawShotData.id)).filter(RawShotData.team == team).scalar() or 0
    subquery = session.query(RawShotData.team, func.count(RawShotData.id).label("shot_count"), func.rank().over(order_by=func.count(RawShotData.id).desc()).label("team_rank")
                  ).group_by(RawShotData.team).order_by(desc("shot_count")).subquery()
    shot_row = session.query(subquery.c.team_rank, subquery.c.shot_count).filter(subquery.c.team == team).first()
    return shot_row
    # return shots

def insert_team_goals(df: pd.DataFrame, session: Session):
    for idx, row in df.iterrows():
        _goal = insert(MatchData).values(
            id = row['match_id'],
            home_team = row['home_team'],
            away_team = row['away_team'],
            home_goals = row['home_score'],
            away_goals = row['away_score'],
            competition = 2
        ).on_conflict_do_nothing()
        session.execute(_goal)
    session.commit()

def insert_team_passes(df: pd.DataFrame, session: Session):
    for idx, row in df.iterrows():
        _pass = insert(PassData).values(
            id = row['id'],
            team = row['team'],
            pass_completed = pd.isna(row['pass_outcome']),
            pass_cross = row['pass_cross'] == True,
            pass_location = row['location'],
            pass_end_location = row['pass_end_location']
        ).on_conflict_do_nothing()
        session.execute(_pass)
    session.commit()

def get_team_crosses(team: str, session: Session):
    crosses = session.query(func.count(PassData.id)).filter(PassData.team == team, PassData.pass_cross == True).scalar() or 0
    return crosses

def get_team_crosses_rank(team: str, session: Session):
    subquery = session.query(PassData.team, func.count(PassData.id).label("cross_count"), func.rank().over(order_by=func.count(PassData.id).desc()).label("team_rank")
                  ).filter(PassData.pass_cross == True).group_by(PassData.team).order_by(desc("cross_count")).subquery()
    crosses_row = session.query(subquery.c.team_rank, subquery.c.cross_count).filter(subquery.c.team == team).first()
    return crosses_row

def get_pass_completion(team: str, session: Session):
    completion_count = session.query(PassData.team.label('team'), func.count(PassData.id).label("completion_count")).filter(PassData.pass_completed == True).group_by(PassData.team).subquery()
    passes_count = session.query(PassData.team.label('team'), func.count(PassData.id).label("passes_count")).group_by(PassData.team).subquery()
    completion_ratio = session.query(completion_count.c.team, (cast(completion_count.c.completion_count, Float) / passes_count.c.passes_count).label('completion_ratio')).join(passes_count, completion_count.c.team == passes_count.c.team).subquery()
    # return session.query(completion_ratio.c.completion_ratio).filter(completion_ratio.c.team == team).first()
    pass_completion_rank = session.query(completion_ratio.c.team, completion_ratio.c.completion_ratio, func.rank().over(order_by=completion_ratio.c.completion_ratio.desc()).label("team_rank")).order_by(desc(completion_ratio.c.completion_ratio)).subquery()
    pass_completion_row = session.query(pass_completion_rank.c.completion_ratio, pass_completion_rank.c.team_rank).filter(pass_completion_rank.c.team == team).first()
    return pass_completion_row

def get_pass_completion_final_third(team: str, session: Session):
    final_third_completion_count = session.query(PassData.team.label('team'), func.count(PassData.id).label("completion_count")).filter(PassData.pass_completed == True, PassData.pass_end_location[0].as_float() >= 80.0).group_by(PassData.team).subquery()
    final_third_count = session.query(PassData.team.label('team'), func.count(PassData.id).label("passes_count")).filter(PassData.pass_end_location[0].as_float() >= 80.0).group_by(PassData.team).subquery()
    completion_ratio = session.query(final_third_completion_count.c.team, (cast(final_third_completion_count.c.completion_count, Float) / final_third_count.c.passes_count).label('completion_ratio')).join(final_third_count, final_third_completion_count.c.team == final_third_count.c.team).subquery()
    # return session.query(completion_ratio.c.completion_ratio).filter(completion_ratio.c.team == team).first()
    pass_completion_rank = session.query(completion_ratio.c.team, completion_ratio.c.completion_ratio, func.rank().over(order_by=completion_ratio.c.completion_ratio.desc()).label("team_rank")).order_by(desc(completion_ratio.c.completion_ratio)).subquery()
    pass_completion_row = session.query(pass_completion_rank.c.completion_ratio, pass_completion_rank.c.team_rank).filter(pass_completion_rank.c.team == team).first()
    return pass_completion_row