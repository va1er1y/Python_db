import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
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
	        email VARCHAR(80) not null
        );
        """)
        conn.commit()
def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO client (first_name, last_name) VALUES (%s, %s) RETURNING id;
        """, (first_name, last_name))
        client_id = cur.fetchone()
        print(client_id[0])
        cur.execute("""
        INSERT INTO phone_number (client_id, phones) VALUES (%s, %s);
        """, (client_id[0], phones))
        cur.execute(f"""
        INSERT INTO email (id, email) VALUES (%s, %s);
        """, (client_id[0], email))

def add_phone(conn, client_id, phones):
    with conn.cursor() as cur:
        cur.execute(f"""
                INSERT INTO phone_number (client_id, phones) VALUES (%s, %s);
                """, (client_id, phones))
        conn.commit()
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE client SET first_name = (%s),last_name = (%s) WHERE id = (%s);
        """, (first_name, last_name, client_id))
        cur.execute("""
        UPDATE phone_number SET phones = (%s) WHERE client_id = (%s);
        """,(phones, client_id))
        cur.execute("""
        UPDATE email SET email = (%s) WHERE id = (%s);
        """,(email, client_id))
        conn.commit()
def delete_phone(conn, phones, client_id=None):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_number WHERE phones = (%s);
        """, (phones,))
        conn.commit()
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_number WHERE client_id = (%s);
        """, (str(client_id)))
        cur.execute("""
        DELETE FROM email WHERE id = (%s);
        """, (str(client_id)))
        cur.execute("""
        DELETE FROM client WHERE id =  (%s);
        """, (str(client_id)))
        conn.commit()
def find_client(conn, first_name, last_name, email, phone):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT c.first_name, c.last_name,  e.email, p.phones
        FROM client c  
        JOIN email e 
        ON c.id = e.id
        JOIN phone_number p 
        ON p.client_id = c.id
        WHERE c.last_name = (%s) OR c.first_name = (%s) OR e.email = (%s) OR p.phones = (%s);
        """, (last_name, first_name, email, phone))
        print(cur.fetchall())

with psycopg2.connect(database="Py_db", user='postgres', host='localhost', password='Va1er1y*SQL123') as conn:
    create_db(conn)
    add_client(conn, "valeriy", "Ivanov", "web@gmail.com", 9200343514)
    add_client(conn, "Zina", "Ivanova", "rzin@gmail.com", 9200343666)
    add_phone(conn, 1, 9200343514)
    # change_client(conn, 1, 'Поликарп', 'Петров', '777@777.ru', 6669996660)
    # delete_phone(conn, 6669996660,)
    # delete_client(conn, 2)
    # find_client(conn, "Zina", "Ivanova", "rzin@gmail.com", 9200343666)
    # find_client(conn, "Zina", None, None, None)
    # find_client(conn, None, None, None, 9200343666)
conn.close()
