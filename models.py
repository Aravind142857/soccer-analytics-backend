from sqlalchemy import Column, Integer, String, JSON, Uuid, Boolean
from .database import Base, engine

class Shots(Base):
    __tablename__ = "shots"

    id = Column(Integer, primary_key=True, index=True)
    team = Column(String, index=True)
    shot_start = Column(JSON)
    shot_end = Column(JSON)
    num_shots = Column(Integer)

class RawShotData(Base):
    __tablename__ = "raw_shot_data"

    id = Column(Uuid, primary_key=True, index=True, unique=True)
    team = Column(String)
    shot_start = Column(JSON)
    shot_end = Column(JSON)

    def to_dict(self):
        return {
            "id": self.id,
            "team": self.team,
            "shot_start": self.shot_start,
            "shot_end": self.shot_end
        }

class RawPressData(Base):
    __tablename__ = "raw_press_data"

    id = Column(Uuid, primary_key=True, index=True, unique=True)
    team = Column(String)
    press_location = Column(JSON)

    def to_dict(self):
        return {
            "id": self.id,
            "team": self.team,
            "press_location": self.press_location
        }

class MatchData(Base):
    __tablename__ = "match_data"

    id = Column(Integer, primary_key=True, index=True, unique=True)
    home_team = Column(String, index=True)
    away_team = Column(String, index=True)
    home_goals = Column(Integer)
    away_goals = Column(Integer)
    competition = Column(String)

class PassData(Base):
    __tablename__ = "pass_data"

    id = Column(Uuid, primary_key=True, index=True, unique=True)
    team = Column(String, index=True)
    pass_completed = Column(Boolean, index=True)
    pass_cross = Column(Boolean, index=True)
    pass_location = Column(JSON)
    pass_end_location = Column(JSON)
Base.metadata.create_all(bind=engine)
