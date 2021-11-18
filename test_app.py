from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy.sql.expression import any_, all_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fill_db import DF, Semantics, SpeechAct, Intonation
from fill_db import main as fill_db
from flask import Flask, render_template, request, redirect, url_for, send_file
import pandas as pd
import re
import time
import os

app = Flask(__name__)

engine = create_engine('postgresql+psycopg2://vantral:prag@127.0.0.1/df')

Base = declarative_base()


class list(list):
    def remove_all(self, element):
        return [el for el in self if el != element]


class set(set):
    def remove_el(self, element):
        self.discard(element)
        return self


def mapper(db_session, table, column, list_of_values):
    if not list_of_values:
        return set()
    local_query = db_session.query(table)
    result_set = set()
    for value in list_of_values:
        result_set.add(local_query.filter(column == value).one().id)
    return result_set


def search_by_parameters(db_session, substring='', inner_structure='', lang=None, syntax='',
                         prim_sem=None, add_sem=None, sp_act=None, structure=None, intonation=None,
                         source='', source_syntax='', source_intonation='', lemmas=[], glosses=[]):
    if substring:
        return db_session.query(DF).filter_by(label=substring)
    
    all_languages = any_(list(set(x.language for x in db_session.query(DF).all())))
    all_semantics = any_([x.semantics for x in db_session.query(Semantics).all()])
    all_speech_acts = any_([x.speech_act for x in db_session.query(SpeechAct).all()])
    all_intonations = any_([x.intonation for x in db_session.query(Intonation).all()])

    lang = any_(lang) if lang else all_languages

    prim_sem_a = any_(prim_sem) if prim_sem else all_semantics
    add_sem_a = any_(add_sem) if add_sem else all_semantics

    sp_act_a = any_(sp_act) if sp_act else all_speech_acts

    intonation_a = any_(intonation) if intonation else all_intonations
    source_intonation = source_intonation if source_intonation else all_intonations
    
    structure = any_([int(x) for x in structure]) if structure else any_([0, 2, 3])

    records = db_session.query(
                            func.array_agg(DF.primary_semantics_id),
                            func.array_agg(DF.additional_semantics_id),
                            func.array_agg(DF.speech_act_id),
                            func.array_agg(DF.speech_act_1_id),
                            func.array_agg(DF.intonation_id),
                            func.array_agg(DF.lemmas),
                            func.array_agg(DF.glosses),
                            DF.label
                        ).group_by(DF.label).filter(
                            DF.language == lang,
                            DF.inner_structure.contains(inner_structure),
                            DF.syntax.contains(syntax),
                            DF.source_construction.contains(source),
                            DF.source_construction_syntax.contains(source_syntax),
                            DF.structure == structure,
                            DF.primary_semantics.has(Semantics.semantics == prim_sem_a),
                            DF.additional_semantics.has(Semantics.semantics == add_sem_a),
                            or_(DF.speech_act.has(SpeechAct.speech_act == sp_act_a),
                                DF.speech_act_1.has(SpeechAct.speech_act == sp_act_a)),
                            DF.lemmas.op('~')('|'.join(lemmas)),
                            DF.glosses.op('~')('|'.join(glosses)),
                            DF.intonation.has(Intonation.intonation == intonation_a)
                        ).all()

    records = [x for x in records if mapper(
        db_session, Semantics, Semantics.semantics, prim_sem
    ).issubset(set(x[0]))]
 
    records = [x for x in records if mapper(
        db_session, Semantics, Semantics.semantics, add_sem
    ).issubset(set(x[1]))]

    records = [x for x in records if mapper(
        db_session, SpeechAct, SpeechAct.speech_act, sp_act
    ).issubset(set(x[2]) | set(x[3]))]

    print(intonation)
    records = [x for x in records if mapper(
        db_session, Intonation, Intonation.intonation, intonation
    ).issubset(set(x[4]))]

    records = [x for x in records if all(lemma in x[5][0] for lemma in lemmas)]
    records = [x for x in records if all(gloss in x[6][0] for gloss in glosses)]


    return db_session.query(DF).filter_by(label=any_([x[-1] for x in records]))


@app.route('/')
def main_page():
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    recs = session.query(DF).all()

    prag_ids = session.query(DF.primary_semantics_id).all()
    prag_ids = set(x[0] for x in prag_ids)
    pragmatics = [session.query(Semantics).filter_by(id=x).one().semantics for x in prag_ids]
   
    add_ids = session.query(DF.additional_semantics_id).all()
    add_ids = set(x[0] for x in add_ids)
    add_sems = [session.query(Semantics).filter_by(id=x).one().semantics for x in add_ids]

    recs_sa = session.query(SpeechAct).all()
    formulae = sorted(list(set(x.label for x in recs)))
    lemmata = sorted(list(set(sum([x.lemmas.replace('|', ' ').split() for x in recs], []))))
    langs = sorted(list(set(x.language for x in recs)))
    in_strucs = sorted(list(set(x.inner_structure for x in recs)))
    pragmatics = sorted(list(set(pragmatics)))
    add_sems = sorted(list(set(add_sems)))
    speech_acts = sorted(list(set(x.speech_act for x in recs_sa)))

    glosses = session.query(DF.glosses).all()
    glosses = set(x[0] for x in glosses)
    glosses = sorted(list(set(y for x in glosses for y in re.split(r'[ .-]', x))))
    
    intonations = session.query(Intonation).all()
    intonations = sorted(list(set(x.intonation for x in intonations)))
    session.close()
    return render_template('index.html', formulae=formulae, lemmata=lemmata,
                           languages=langs, strucs=in_strucs, add_sems=add_sems,
                           speech_acts=speech_acts, pragmatics=pragmatics,
                           intonations=intonations, glosses=glosses)


@app.route('/result', methods=['get'])
def result():
    if not request.args:
        return redirect(url_for('main_page'))

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    substring = request.args.get('word')
    pragmatics = request.args.getlist('pragmatics')
    add_sem = request.args.getlist('add_sem')
    lemmas = request.args.get('lemma')
    if lemmas:
        lemmas = lemmas.split()
    glosses = request.args.getlist('glosses')
    lang = request.args.getlist('language')
    syntax = request.args.get('syntax')
    inner = request.args.get('inner')
    speech_act = request.args.getlist('speech_act')
    structure = request.args.getlist('structure')
    intonation = request.args.getlist('intonation')

    results = search_by_parameters(session, substring=substring, inner_structure=inner, lang=lang, syntax=syntax,
                        prim_sem=pragmatics, add_sem=add_sem, sp_act=None, structure=structure, intonation=intonation,
                        source='', source_syntax='', source_intonation='', lemmas=lemmas, glosses=glosses)
    
    if not [x.__dict__ for x in results]:
        return render_template('oops.html')
    df = pd.DataFrame([x.__dict__ for x in results])

    df = df.groupby(['label', 'primary_semantics_id'])
    pretty_records = []
    db_session = session
    for record in df:
        base = record[0]
        df = record[1]
        # print(df.primary_semantics_id)
        glossing = list(set(zip(df['df'], df['glosses']))).remove_all(('', ''))
        glossing = [[x, y] for x, y in glossing]
        if len(glossing) == 1 and not glossing[0][1]:
            glossing = []
        pretty_records.append(
            {
                'label': base[0],
                'Glossing': glossing,
                'Lemmas': '|'.join(df['lemmas']),
                'Inner structure': df['inner_structure'].to_list()[0],
                'Language': df.language.to_list()[0],
                'Syntactic structure': df.syntax.to_list()[0],
                'Pragmatics': db_session.query(Semantics).filter_by(id=int(df.primary_semantics_id.to_list()[0])).one().semantics,
                'Additional semantics': list(set([db_session.query(Semantics).filter_by(id=int(x)).one().semantics
                                         for x in df.additional_semantics_id.to_list()])),
                'Speech act 1': ' | '.join(set([db_session.query(SpeechAct).filter_by(id=int(df.speech_act_1_id.to_list()[i])).one().speech_act for i in range(len(
                    df.speech_act_1_id.to_list()
                ))]).remove_el('')),
                'Speech act': ' | '.join(set([db_session.query(SpeechAct).filter_by(id=int(df.speech_act_id.to_list()[i])).one().speech_act for i in range(len(
                    df.speech_act_id.to_list()
                ))]).remove_el('')),
                'Structure': 'bipartite' if df.structure.to_list()[0] == 2 else 'tripartite' if df.structure.to_list()[0] == 3 else '',
                'Intonation': ' | '.join(
                    set(db_session.query(Intonation).filter_by(id=int(df.intonation_id.to_list()[x])).one().intonation for x in range(len(df.intonation_id.to_list())))
                ),
                'Source construction': df.source_construction.to_list()[0],
                'Source construction syntactic structure': df.source_construction_syntax.to_list()[0],
                'Source construction intonation': db_session.query(Intonation).filter_by(
                    id=int(df.source_construction_intonation_id.to_list()[0])).one().intonation,
                'examples': df.examples.to_list()[0].replace('{', '<b>').replace('}', '</b>'),
                'comments': df.comments.to_list()[0]
            }
        )
    records = sorted(pretty_records, key=lambda x: (x['Language'], x['label']))
    
    name = f'./files/{time.time()}.xlsx'
    pd.DataFrame(records).to_excel(name)
    
    session.close()

    return render_template('result.html', records=records,
                           isinst=isinstance, lst=list, empty={''}, file=name)


@app.route('/about')
def about():
    return render_template('wip.html')


@app.route('/instruction')
def instruction():
    return render_template('wip.html')


@app.route('/publications')
def publications():
    return render_template('wip.html')


@app.route('/download', methods=['GET'])
def create_file():
    path = request.args.get('path')
    response = send_file(path, attachment_filename='Sample.xlsx', as_attachment=True)
    response.headers["x-filename"] = 'Sample.xlsx'
    response.headers["Access-Control-Expose-Headers"] = 'x-filename'
    os.remove(path)
    return response


@app.route('/update')
def fill():
    fill_db()
    return redirect(url_for('main_page'))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)