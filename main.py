import psycopg2

def create_db(cur):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client (
	        PRIMARY key (id),
	        id          SERIAL,
	        first_name        VARCHAR(40) NOT NULL,
	        last_name         VARCHAR(40) NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_number (
	        PRIMARY key (id),
	        id          SERIAL,
	        client_id INTEGER NOT NULL REFERENCES client(id),
	        phones      DECIMAL(11)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS email (
	        id    INTEGER PRIMARY KEY REFERENCES client(id),
	        email VARCHAR(80)
        );
        """)

def add_client(cur, first_name, last_name, email, phones=None):
        cur.execute("""
        INSERT INTO client (first_name, last_name) VALUES (%s, %s) RETURNING id;
        """, (first_name, last_name))
        client_id = cur.fetchone()
        cur.execute("""
        INSERT INTO phone_number (client_id, phones) VALUES (%s, %s);
        """, (client_id[0], phones))
        cur.execute(f"""
        INSERT INTO email (id, email) VALUES (%s, %s);
        """, (client_id[0], email))

def add_phone(cur, client_id, phones):
        cur.execute(f"""
                INSERT INTO phone_number (client_id, phones) VALUES (%s, %s);
                """, (client_id, phones))

def change_client(cur, parameter):
        if parameter['имя'] is not None:
            cur.execute("""
            UPDATE client SET first_name = (%s) WHERE id = (%s);
            """, (parameter['имя'], parameter['id']))
        if parameter['фамилия'] is not None:
            cur.execute("""
            UPDATE client SET last_name = (%s) WHERE id = (%s);
            """, (parameter['фамилия'], parameter['id']))
        if parameter['телефон'] is not None:
            cur.execute("""
            UPDATE phone_number SET phones = (%s) WHERE client_id = (%s);
            """,(parameter['телефон'], parameter['id']))
        if parameter['email'] is not None:
            cur.execute("""
            UPDATE email SET email = (%s) WHERE id = (%s);
            """,(parameter['email'], parameter['id']))
def delete_phone(cur, phones):
        cur.execute("""
        DELETE FROM phone_number WHERE phones = (%s);
        """, (phones,))

def delete_client(cur, client_id):
        cur.execute("""
        DELETE FROM phone_number WHERE client_id = (%s);
        """, (str(client_id)))
        cur.execute("""
        DELETE FROM email WHERE id = (%s);
        """, (str(client_id)))
        cur.execute("""
        DELETE FROM client WHERE id =  (%s);
        """, (str(client_id)))

def find_client(cur, parameter):
        a = tuple()
        b = None
        name_parameters = {'имя':'c.first_name', 'фамилия':'c.last_name', 'телефон':'p.phones',
                           'email':'e.email', 'id':'c.id'}
        for k, v in parameter.items():
            if  b is None:
                if v is not None:
                    for k2, v2 in name_parameters.items():
                        if k == k2:
                            a = (v2, v)
                            b = v
                            break
            else:
                break

        cur.execute("""
               SELECT c.first_name, c.last_name,  e.email, p.phones, c.id
               FROM client c
               LEFT JOIN email e
               ON c.id = e.id
               LEFT JOIN phone_number p
               ON p.client_id = c.id
               WHERE  {} = {};
               """.format(str(a[0]), str(a[1])))
        print(cur.fetchall())

if __name__ == "__main__":
    with psycopg2.connect(database="Py_db", user='postgres', host='localhost', password='Va1er1y*SQL123') as conn:
        with conn.cursor() as cur:
            create_db(cur)
            add_client(cur, "valeriy", "Ivanov", "web@gmail.com", 9200343514)
            add_client(cur, "Zina", "Ivanova", "rzin@gmail.com", 9200343666)
            add_phone(cur, 1, 9200343514)
            change_client(cur, {'id':1, 'имя': 'АркадиЙ', 'фамилия':None, 'телефон':None, 'email':None})
            delete_phone(cur, 9200343666)
            find_client(cur, {'телефон':9200343514})
            find_client(cur, {'id': 2})
            find_client(cur, {'фамилия':"'Ivanova'", 'email':"'rzin@gmail.com'"})
            conn.commit()
    conn.close()
