from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.schema import Sequence
from sqlalchemy.sql.expression import func
import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
from conf import ENGINE


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    'pragmaticon.json', scope
)
gc = gspread.authorize(credentials)

engine = create_engine(ENGINE)

Base = declarative_base()

MAPPING = {
    'id': 'id',
    'DF': 'label',
    'realisation': 'df',
    'inner structure type': 'inner_structure',
    'inner structure subtype': 'inner_structure_subtype',
    'language': 'language',
    'glosses': 'glosses',
    'examples': 'examples',
    'lemmas': 'lemmas',
    'syntax': 'syntax',
    'primary semantics': 'primary_semantics_id',
    'additional semantics': 'additional_semantics_id',
    'speech act 1': 'speech_act_1_id',
    'speech act': 'speech_act_id',
    'structure': 'structure',
    'intonation': 'intonation_id',
    'source construction': 'source_construction',
    'SC syntax': 'source_construction_syntax',
    'SC intonation': 'source_construction_intonation_id',
    'comments': 'comments'
}

class Intonation(Base):
    __tablename__ = 'intonations'

    id = Column(Integer, primary_key=True)
    intonation = Column(String)


class Semantics(Base):
    __tablename__ = 'semantics'

    id = Column(Integer, primary_key=True, autoincrement='auto')
    semantics = Column(String)


class SpeechAct(Base):
    __tablename__ = 'speech_acts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    speech_act = Column(String)


class DF(Base):
    __tablename__ = 'dfs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    label = Column(String)
    df = Column(String)
    inner_structure = Column(String)
    inner_structure_subtype = Column(String)
    language = Column(String)
    glosses = Column(String)
    lemmas = Column(String)
    syntax = Column(String)
    primary_semantics_id = Column(Integer, ForeignKey('semantics.id'))
    primary_semantics = relationship("Semantics",
                                     foreign_keys=[primary_semantics_id])
    additional_semantics_id = Column(Integer, ForeignKey('semantics.id'))
    additional_semantics = relationship("Semantics",
                                        foreign_keys=[additional_semantics_id])
    speech_act_1_id = Column(Integer, ForeignKey("speech_acts.id"))
    speech_act_1 = relationship("SpeechAct", foreign_keys=[speech_act_1_id])
    speech_act_id = Column(Integer, ForeignKey("speech_acts.id"))
    speech_act = relationship("SpeechAct", foreign_keys=[speech_act_id])
    structure = Column(Integer)
    intonation_id = Column(Integer, ForeignKey("intonations.id"))
    intonation = relationship("Intonation", foreign_keys=[intonation_id])
    source_construction = Column(String)
    source_construction_syntax = Column(String)
    source_construction_intonation_id = Column(
        Integer, ForeignKey("intonations.id")
    )
    source_construction_intonation = relationship(
        "Intonation", foreign_keys=[source_construction_intonation_id]
    )
    examples = Column(String)
    comments = Column(String)


def split_data(dataframe):
    new_formulas: pd.DataFrame = dataframe[dataframe['status'] == 'to_db']
    edit_formulas = dataframe[dataframe['status'] == 'change']
    delete_formulas = dataframe[dataframe['status'] == 'delete']
    return new_formulas, edit_formulas, delete_formulas


def duplify_rows(final_df, colname):
    final_df[colname] = final_df[colname].apply(
        lambda x: [y.strip() for y in x.split('|')] if '|' in x else x
        )
    final_df = final_df.explode(colname)
    return final_df


def create_instances(df_from_table):
    df = df_from_table[df_from_table.columns.difference(['status'])]
    df = df.rename(MAPPING, axis=1)
    df['structure'] = df['structure'].apply(lambda x: 0 if not x else x)
    for el in ['additional_semantics_id', 'speech_act_1_id', 'speech_act_id']:
        df = duplify_rows(df, el)
    return df


def clean_formulas(df, db_session, engine):

    semantics = set([
            x.semantics for x in db_session.query(Semantics).all()
        ])
    
    speech_acts = set([
            x.speech_act for x in db_session.query(SpeechAct).all()
        ])

    intonations = set([
            x.intonation for x in db_session.query(Intonation).all()
        ])

    primary_semantics = set(df['primary_semantics_id'])
    additional_semantics = set(df['additional_semantics_id'])
    lack_semantics = (primary_semantics | additional_semantics) - semantics
    semantics_to_add = pd.DataFrame(lack_semantics, columns=['semantics'])
    max_value = db_session.query(func.max(Semantics.id)).one()[0]
    if not max_value:
        max_value = 0
    max_value += 1
    semantics_to_add['id'] = list(range(max_value, max_value + len(semantics_to_add)))
    semantics_to_add.to_sql('semantics', con=engine, if_exists='append', index=False)

    sp_act_1 = set(df['speech_act_1_id'])
    sp_act = set(df['speech_act_id'])
    lack_speech_acts = (sp_act | sp_act_1) - speech_acts
    sp_acts_to_add = pd.DataFrame(lack_speech_acts, columns=['speech_act'])
    max_value = db_session.query(func.max(SpeechAct.id)).one()[0]
    if not max_value:
        max_value = 0
    max_value += 1
    sp_acts_to_add['id'] = list(range(max_value, max_value + len(sp_acts_to_add)))
    sp_acts_to_add.to_sql('speech_acts', con=engine, if_exists='append', index=False)

    intonation = set(df['intonation_id'])
    sci = set(df['source_construction_intonation_id'])
    lack_intonations = (intonation | sci) - intonations
    intonations_to_add = pd.DataFrame(lack_intonations, columns=['intonation'])
    max_value = db_session.query(func.max(Intonation.id)).one()[0]
    if not max_value:
        max_value = 0
    max_value += 1
    intonations_to_add['id'] = list(range(max_value, max_value + len(intonations_to_add)))
    intonations_to_add.to_sql('intonations', con=engine, if_exists='append', index=False)

    df[['primary_semantics_id', 'additional_semantics_id']] = df[
        ['primary_semantics_id', 'additional_semantics_id']
        ].applymap(lambda x: db_session.query(Semantics).filter_by(semantics=x).one().id)

    df[['speech_act_1_id', 'speech_act_id']] = df[
        ['speech_act_1_id', 'speech_act_id']
        ].applymap(lambda x: db_session.query(SpeechAct).filter_by(speech_act=x).one().id)
    
    df[['intonation_id', 'source_construction_intonation_id']] = df[
        ['intonation_id', 'source_construction_intonation_id']
        ].applymap(lambda x: db_session.query(Intonation).filter_by(intonation=x).one().id)

    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

    return df


def add_to_db(df, db_session, engine):
    clean_fs = clean_formulas(df, db_session, engine)
    max_value = db_session.query(func.max(DF.id)).one()[0]
    if not max_value:
        max_value = 0
    max_value += 1
    clean_fs['id'] = list(range(max_value, max_value + len(clean_fs)))
    clean_fs.to_sql('dfs', con=engine, if_exists='append', index=False)


def change_in_db(instances, db_session):
    clean_fs = clean_formulas(instances, db_session, engine)

    labels = set(clean_fs['label'])
    db_session.query(DF).filter(DF.label.in_(labels)).delete()
    db_session.commit()

    max_value = db_session.query(func.max(DF.id)).one()[0]
    if not max_value:
        max_value = 0
    max_value += 1
    clean_fs['id'] = list(range(max_value, max_value + len(clean_fs)))
    clean_fs.to_sql('dfs', con=engine, if_exists='append', index=False)


def delete_from_db(instances, db_session):
    clean_fs = clean_formulas(instances, db_session, engine)

    labels = set(clean_fs['label'])
    db_session.query(DF).filter(DF.label.in_(labels)).delete()
    db_session.commit()


def all_done(dataframe, key, creds):
    df = dataframe['status'].apply(
        lambda x: 'deleted' if x == 'delete' else 'done'
    )
    d2g.upload(
        pd.DataFrame(df),
        key, 'Лист1', clean=False, start_cell='T1',
        credentials=creds, row_names=False
    )


def main():
    Base.metadata.create_all(engine)
    print('hello')

    Session = sessionmaker(bind=engine)
    session = Session()

    sheet_url = 'https://docs.google.com/spreadsheets/d' \
                '/1kyesqJ3k2WFmygRq7R1iL2ZC7yM-XNQ5Shye2rdEAL0/edit#gid' \
                '=67313858 '
    url = sheet_url.replace('/edit#gid=', '/export?format=csv&gid=')

    formulas = pd.read_csv(url).fillna('')

    to_db, change, delete = split_data(formulas)

    formulas_to_db = create_instances(to_db)
    formulas_to_change = create_instances(change)
    formulas_to_delete = create_instances(delete)

    print('here')
    add_to_db(formulas_to_db, session, engine)
    print('added')
    change_in_db(formulas_to_change, session)
    print('changed')
    delete_from_db(formulas_to_delete, session)

    session.close()

    all_done(formulas,
             '1kyesqJ3k2WFmygRq7R1iL2ZC7yM-XNQ5Shye2rdEAL0',
             credentials)


if __name__ == '__main__':
    main()

