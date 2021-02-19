import psycopg2
import pandas as pd
from pprint import pprint

formulas = pd.read_excel('sample.xlsx').fillna('')
formulas = formulas.groupby(['DF', 'primary semantics', 'speech act'])

# print(pd.DataFrame(formulas))

formulas_to_db = []

for formula in formulas:
    df = formula[1]
    formulas_to_db.append(
        [
            '|'.join(df.realisation),
            df.language.iloc[0],
            df.glosses.iloc[0],
            df.syntax.iloc[0],
            df['primary semantics'].iloc[0],
            df['additional semantics'].iloc[0],
            df['speech act 1'].iloc[0],
            df['speech act'].iloc[0],
            df.structure.iloc[0],
            df.intonation.iloc[0],
            df["source construction"].iloc[0],
            df["SC syntax"].iloc[0],
            df["SC intonation"].iloc[0],
            df.examples.iloc[0],
            df.comments.iloc[0]
        ]
    )

print(formulas_to_db)

conn = psycopg2.connect(dbname='postgres', user='postgres',
                        password='admin', host='localhost')
cursor = conn.cursor()

cursor.execute('SELECT * FROM DFs')
all_fetches = [x[1:] for x in cursor.fetchall()]

for line in formulas_to_db:
    prim_sem = [line[4]]
    add_sem = [x.strip() for x in line[5].split('|')]
    cursor.execute('SELECT semantics from semantics')
    sems = set([x[0] for x in cursor.fetchall()])
    lack_sems = set(prim_sem + add_sem) - sems
    for sem in lack_sems:
        cursor.execute('INSERT INTO semantics VALUES (DEFAULT, %s)', (sem,))

    s_acts = {line[6], line[7]}
    cursor.execute('SELECT speech_act from speech_acts')
    pr_s_acts = set([x[0] for x in cursor.fetchall()])
    lack_s_acts = s_acts - pr_s_acts
    for act in lack_s_acts:
        cursor.execute('INSERT INTO speech_acts VALUES (DEFAULT, %s)', (act,))

    intonations = {line[9], line[12]}
    cursor.execute('SELECT intonation from intonations')
    pr_ints = set([x[0] for x in cursor.fetchall()])
    lack_ints = intonations - pr_ints
    for intonation in lack_ints:
        print(intonation)
        cursor.execute('INSERT INTO intonations VALUES (DEFAULT, %s)', (str(intonation),))

    cursor.execute('SELECT id from semantics WHERE semantics = %s', (prim_sem[0],))
    line[4] = cursor.fetchone()[0]

    cursor.execute('SELECT id from semantics WHERE semantics = ANY(%s)', (add_sem,))
    line[5] = '|'.join([str(x[0]) for x in cursor.fetchall()])

    cursor.execute('SELECT id from speech_acts WHERE speech_act = %s', (line[6],))
    line[6] = cursor.fetchone()[0]

    cursor.execute('SELECT id from speech_acts WHERE speech_act = %s', (line[7],))
    line[7] = cursor.fetchone()[0]

    cursor.execute('SELECT id from intonations WHERE intonation = %s', (line[9],))
    line[9] = cursor.fetchone()[0]

    cursor.execute('SELECT id from intonations WHERE intonation = %s', (str(line[12]),))
    line[12] = cursor.fetchone()[0]

    line = tuple([str(x) for x in line])

    if line not in all_fetches:
        print(line)
        cursor.execute('INSERT INTO DFs VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                       line)

    conn.commit()


