from copy import copy

from sqlalchemy import create_engine
from sqlalchemy.sql.expression import any_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
import pandas as pd

engine = create_engine('postgresql+psycopg2://postgres:admin@localhost/postgres')

Base = declarative_base()


# описали с помощью класса таблицу users
class Intonation(Base):
    __tablename__ = 'intonations'

    # метакласс превратит всю эту шляпу в нормальные атрибуты
    id = Column(Integer, primary_key=True, autoincrement=True)
    intonation = Column(String)


class Semantics(Base):
    __tablename__ = 'semantics'

    # метакласс превратит всю эту шляпу в нормальные атрибуты
    id = Column(Integer, primary_key=True, autoincrement=True)
    semantics = Column(String)


class SpeechAct(Base):
    __tablename__ = 'speech_acts'

    # метакласс превратит всю эту шляпу в нормальные атрибуты
    id = Column(Integer, primary_key=True, autoincrement=True)
    speech_act = Column(String)


class DF(Base):
    __tablename__ = 'dfs'

    # метакласс превратит всю эту шляпу в нормальные атрибуты
    id = Column(Integer, primary_key=True, autoincrement=True)
    df = Column(String)
    language = Column(String)
    glosses = Column(String)
    syntax = Column(String)
    primary_semantics_id = Column(String)
    additional_semantics_id = Column(String)
    speech_act_1_id = Column(String)
    speech_act_id = Column(String)
    structure = Column(String)
    intonation_id = Column(String)
    source_construction = Column(String)
    source_construction_syntax = Column(String)
    source_construction_intonation_id = Column(String)
    examples = Column(String)
    comments = Column(String)

    def __eq__(self, other: "DF"):
        # print([self.df, other.df],
        #       [self.language, other.language],
        #       [self.glosses, other.glosses],
        #       [self.syntax, other.syntax],
        #       [self.primary_semantics_id, other.primary_semantics_id],
        #       [self.additional_semantics_id, other.additional_semantics_id],
        #       [self.speech_act_id, other.speech_act_id],
        #       [self.speech_act_1_id, other.speech_act_1_id],
        #       [self.structure, other.structure],
        #       [self.intonation_id, other.intonation_id],
        #       [self.source_construction, other.source_construction],
        #       [self.source_construction_syntax, other.source_construction_syntax],
        #       [self.source_construction_intonation_id, other.source_construction_intonation_id],
        #       [self.examples, other.examples],
        #       [self.comments, other.comments])

        return str(self.df) == str(other.df) and str(self.language) == str(other.language) \
               and str(self.glosses) == str(other.glosses) and str(self.syntax) == str(other.syntax) \
               and str(self.primary_semantics_id) == str(other.primary_semantics_id) \
               and str(self.additional_semantics_id) == str(other.additional_semantics_id) \
               and str(self.speech_act_id) == str(other.speech_act_id) and str(self.speech_act_1_id) == str(
            other.speech_act_1_id) \
               and str(self.structure) == str(other.structure) and str(self.intonation_id) == str(other.intonation_id) \
               and str(self.source_construction) == str(other.source_construction) \
               and str(self.source_construction_syntax) == str(other.source_construction_syntax) \
               and str(self.source_construction_intonation_id) == str(other.source_construction_intonation_id) \
               and str(self.examples) == str(other.examples) and str(self.comments) == str(other.comments)


def exemplar_in_collection(exemplar, collection):
    for example in collection:
        if example == exemplar:
            return True
    return False


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

all_formulas = session.query(DF).all()

if all_formulas:
    counter = all_formulas[-1].id + 1
else:
    counter = 1

formulas_to_db = []
formulas = pd.read_excel('sample.xlsx').fillna('')
formulas = formulas.groupby(['DF', 'primary semantics', 'speech act'])



for formula in formulas:
    df = formula[1]
    formulas_to_db.append(
        {
            'df': '|'.join(df.realisation),
            'language': df.language.iloc[0],
            'glosses': df.glosses.iloc[0],
            'syntax': df.syntax.iloc[0],
            'primary_semantics_id': df['primary semantics'].iloc[0],
            'additional_semantics_id': df['additional semantics'].iloc[0],
            'speech_act_1_id': df['speech act 1'].iloc[0],
            'speech_act_id': df['speech act'].iloc[0],
            'structure': df.structure.iloc[0],
            'intonation_id': df.intonation.iloc[0],
            'source_construction': df["source construction"].iloc[0],
            'source_construction_syntax': df["SC syntax"].iloc[0],
            'source_construction_intonation_id': df["SC intonation"].iloc[0],
            'examples': df.examples.iloc[0],
            'comments': df.comments.iloc[0]
        }
    )

for line in formulas_to_db:
    prim_sem = [line['primary_semantics_id']]
    add_sem = [x.strip() for x in line['additional_semantics_id'].split('|')]
    sems = set([x.semantics for x in session.query(Semantics).all()])
    lack_sems = set(prim_sem + add_sem) - sems
    for sem in lack_sems:
        nums = session.query(Semantics).all()
        if nums:
            num = nums[-1].id
        else:
            num = 0
        element = Semantics(id=num+1, semantics=sem)
        session.add(element)

    s_acts = {line['speech_act_1_id'], line['speech_act_id']}
    pr_s_acts = set([x.speech_act for x in session.query(SpeechAct).all()])
    lack_s_acts = s_acts - pr_s_acts
    for act in lack_s_acts:
        nums = session.query(SpeechAct).all()
        if nums:
            num = nums[-1].id
        else:
            num = 0
        element = SpeechAct(id=num+1, speech_act=act)
        session.add(element)

    intonations = {line['intonation_id'], line['source_construction_intonation_id']}
    pr_ints = set([x.intonation for x in session.query(Intonation).all()])
    lack_ints = intonations - pr_ints
    for intonation in lack_ints:
        nums = session.query(Intonation).all()
        if nums:
            num = nums[-1].id
        else:
            num = 0
        element = Intonation(id=num+1, intonation=intonation)
        session.add(element)

    ps = session.query(Semantics).filter_by(semantics=prim_sem[0]).one()
    line['primary_semantics_id'] = ps.id

    ads = session.query(Semantics).filter_by(semantics=any_(add_sem)).all()
    line['additional_semantics_id'] = '|'.join([str(x.id) for x in ads])

    sas = session.query(SpeechAct).filter_by(speech_act=line['speech_act_1_id']).one()
    line['speech_act_1_id'] = sas.id

    sa_1s = session.query(SpeechAct).filter_by(speech_act=line['speech_act_id']).one()
    line['speech_act_id'] = sa_1s.id

    intonations = session.query(Intonation).filter_by(intonation=str(line['intonation_id'])).one()
    line['intonation_id'] = intonations.id

    intonantions_source = session.query(Intonation).filter_by(
        intonation=str(line['source_construction_intonation_id'])
    ).all()

    line['source_construction_intonation_id'] = intonantions_source[0].id

    line = DF(**line)
    if not exemplar_in_collection(line, all_formulas):
        line.id = counter
        counter += 1
        session.add(line)

    session.commit()
