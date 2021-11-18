from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy.sql.expression import any_, all_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fill_db import DF, Semantics, SpeechAct, Intonation
from fill_db import main as fill_db
from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import re


engine = create_engine('postgresql+psycopg2://vantral:prag@127.0.0.1/df')

Base = declarative_base()

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# x = session.query(func.array_agg(DF.label)).group_by(DF.label).filter(or_(DF.primary_semantics_id==11, DF.primary_semantics_id==3)).all()

# x = session.query(DF.label).group_by(DF.label, DF.primary_semantics_id).filter(and_(DF.primary_semantics_id==11, DF.primary_semantics_id==3)).all()

x = session.query(func.array_agg(DF.primary_semantics_id), DF.label).group_by(DF.label).filter(or_(DF.primary_semantics_id==3, DF.primary_semantics_id==11)).all()


print([x for x in x if {3, 11}.issubset(set(x[0]))])
