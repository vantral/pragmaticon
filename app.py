from sqlalchemy import create_engine
from sqlalchemy.sql.expression import any_, all_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fill_db import DF, Semantics, SpeechAct, Intonation
from fill_db import main as fill_db
from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import re

app = Flask(__name__)

engine = create_engine('postgresql+psycopg2://vantral:prag@127.0.0.1/df')

Base = declarative_base()


def get_all_formulas(db_session):
    all_formulas = []
    all_instances = db_session.query(DF).all()
    for instance in all_instances:
        all_formulas.extend(instance.split('|'))
    return any_(list(set(all_formulas)))


def find_formula(db_session, search_results):
    formulas = []
    identifications = [x.id for x in search_results]
    df = pd.read_sql(db_session.query(DF).statement, db_session.bind)
    df = df.groupby(['df', 'primary_semantics_id', 'speech_act_id'])
    for group in df:
        for ident in identifications:
            if ident in group[1]['id'].values:
                formulas.append(group[1])
                break
    return formulas


def keep_only_duplicates(list_of_records):
    labels = [x['label'] for x in list_of_records]
    return [x for x in list_of_records if labels.count(x['label']) != 1]


def search_by_parameters(db_session, substring='', inner_structure='', lang=None, syntax='',
                         prim_sem=None, add_sem=None, sp_act_1=None, sp_act=None, structure=None, intonation=None,
                         source='', source_syntax='', source_intonation=None):
    all_languages = any_(list(set(x.language for x in db_session.query(DF).all())))
    all_semantics = any_([x.semantics for x in db_session.query(Semantics).all()])
    all_speech_acts = any_([x.speech_act for x in db_session.query(SpeechAct).all()])
    all_intonations = any_([x.intonation for x in db_session.query(Intonation).all()])

    lang = lang if lang is not None else all_languages
    prim_sem = prim_sem if prim_sem is not None else all_semantics

    add_sem = add_sem if add_sem is not None else all_semantics
    sp_act = sp_act if sp_act is not None else all_speech_acts
    sp_act_1 = sp_act_1 if sp_act_1 is not None else all_speech_acts
    intonation = intonation if intonation is not None else all_intonations
    source_intonation = source_intonation if source_intonation is not None else all_intonations
    structure = structure if structure is not None else any_([0, 1, 2])

    new_query = db_session.query(DF)

    if substring:
        return new_query.filter(DF.label == substring)
    records = new_query.filter(DF.language == lang,
                               DF.df.contains(substring),
                               DF.inner_structure.contains(inner_structure),
                               DF.syntax.contains(syntax),
                               DF.source_construction.contains(source),
                               DF.source_construction_syntax.contains(source_syntax),
                               DF.structure == structure,
                               DF.primary_semantics.has(Semantics.semantics == prim_sem),
                               DF.additional_semantics.has(Semantics.semantics == add_sem),
                               DF.speech_act.has(SpeechAct.speech_act == sp_act),
                               DF.speech_act_1.has(SpeechAct.speech_act == sp_act_1)
                               ).all()
    return records


def drop_empty_lists(lst):
    for i in lst:
        if i:
            return lst
    return []


def prettify_records(raw_records, db_session, flag, glosses, lemmas):
    pretty_records = []
    for record in raw_records:
        base = record.iloc[0,]
        pretty_records.append(
            {
                'label': base.label,
                'Realisations': drop_empty_lists(base.df.split('|')),
                'Glosses': drop_empty_lists(base.glosses.split('|')),
                'Lemmas': drop_empty_lists(base.lemmas.split('|')),
                'Inner structure': base.inner_structure,
                'Language': base.language,
                'Syntactic structure': base.syntax,
                'Pragmatics': db_session.query(Semantics).filter_by(id=int(base.primary_semantics_id)).one().semantics,
                'Additional semantics': [db_session.query(Semantics).filter_by(id=int(x)).one().semantics
                                         for x in list(record.additional_semantics_id)],
                'Speech act 1': db_session.query(SpeechAct).filter_by(id=int(base.speech_act_1_id)).one().speech_act,
                'Speech act': db_session.query(SpeechAct).filter_by(id=int(base.speech_act_id)).one().speech_act,
                'Structure': 'bipartite' if base.structure == '2' else 'tripartite' if base.structure == '3' else '',
                'Intonation': db_session.query(Intonation).filter_by(id=int(base.intonation_id)).one().intonation,
                'Source construction': base.source_construction,
                'Source construction syntactic structure': base.source_construction_syntax,
                'Source construction intonation': db_session.query(Intonation).filter_by(
                    id=int(base.source_construction_intonation_id)).one().intonation,
                'examples': base.examples.split(';'),
                'comments': base.comments.split(';')
            }
        )
    if glosses:
        gloss_records = []
        current_set = set(glosses)
        for lst in pretty_records:
            word_set = set()
            for gloss in lst['Glosses']:
                word_set.update(set([x.strip() for x in re.split(r'[.: -]', gloss)]))
            print(current_set)
            print(word_set)
            if current_set.issubset(word_set):
                gloss_records.append(lst)
        pretty_records = [x for x in gloss_records]
    if lemmas:
        lemma_records = []
        current_set = set(lemmas)
        for lst in pretty_records:
            word_set = set()
            for lemma in lst['Lemmas']:
                word_set.update(set([x.strip() for x in lemma.split()]))
            print(current_set)
            print(word_set)
            if current_set.issubset(word_set):
                lemma_records.append(lst)
        pretty_records = [x for x in lemma_records]
    if flag:
        return keep_only_duplicates(pretty_records)
    return pretty_records



@app.route('/')
def main_page():
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    recs = session.query(DF).all()
    recs_sem = session.query(Semantics).all()
    recs_sa = session.query(SpeechAct).all()
    formulae = sorted(list(set(x.label for x in recs)))
    lemmata = sorted(list(set(sum([x.lemmas.replace('|', ' ').split() for x in recs], []))))
    langs = sorted(list(set(x.language for x in recs)))
    in_strucs = sorted(list(set(x.inner_structure for x in recs)))
    pragmatics = sorted(list(set(x.semantics for x in recs_sem)))
    add_sems = pragmatics
    speech_acts = sorted(list(set(x.speech_act for x in recs_sa)))
    session.close()
    return render_template('index.html', formulae=formulae, lemmata=lemmata,
                           languages=langs, strucs=in_strucs, add_sems=add_sems,
                           speech_acts=speech_acts, pragmatics=pragmatics)


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

    results = search_by_parameters(session, substring=substring,
                                   prim_sem=pragmatics, add_sem=add_sem,
                                   lemmas=lemmas, glosses=glosses,
                                   lang=lang, syntax=syntax,
                                   inner_structure=inner, sp_act=speech_act,
                                   structure=structure, intonation=intonation)

    # results = find_formula(session, results)
    # records = prettify_records(results, session, flag,
    #                            glosses=glosses, lemmas=lemmas)

    # records = sorted(records, key=lambda x: (x['Language'], x['label']))
    session.close()

    return render_template('result.html', records=records,
                           isinst=isinstance, lst=list)


@app.route('/update')
def fill():
    fill_db()
    return redirect(url_for('main_page'))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
