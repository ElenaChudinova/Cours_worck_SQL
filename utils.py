import os
import psycopg2
import csv


"""Скрипт для заполнения данными таблиц в БД Postgres."""
con = psycopg2.connect(host='localhost', database='north', user='postgres', password='0000')

path = os.path.join(os.path.dirname(__file__), "north_data/employees_data.csv")
try:
    with con:
        with con.cursor() as cur:
            with open(path, 'r', encoding='UTF-8') as file:
                reader = csv.DictReader(file)
                for i in reader:
                    employee_id = i['employee_id'],
                    first_name = i['first_name'],
                    last_name = i['last_name'],
                    title = i['title'],
                    birth_date = i['birth_date'],
                    notes = i['notes']
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", (
                            employee_id,
                            first_name,
                            last_name,
                            title,
                            birth_date,
                            notes))
                    cur.execute("SELECT * FROM employees")

                    datas = cur.fetchall()
                    for data in datas:
                        print(data)

finally:
    con.close()