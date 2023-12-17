import psycopg2


class DBManager:

    def __init__(self, params, database_name):
        # Инициализатор класса для подключения к БД Postgres и автосохранение новой информации
        self.create_dbbase(params, database_name)
        self.conn = psycopg2.connect(dbname=database_name, **params)

    def create_dbbase(self, params, database_name):
        # создание базы данных
        self.conn = psycopg2.connect(dbname='postgres', **params)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
        self.cur.execute(f'CREATE DATABASE {database_name}')
        self.cur.close()
        self.conn.close()

    def create_table_employee(self):
        # создание таблицы по компаниям
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE employee
            (employee_id INT PRIMARY KEY NOT NULL,
            employee_name VARCHAR(255) NOT NULL)
        """)

    def create_table_vacancies(self):
        # создание таблицы по вакансиям
        with self.conn.cursor() as cur:
            cur.execute("""
                  CREATE TABLE vacancies
                  (vacancies_id INT PRIMARY KEY NOT NULL,
                  employee_id INT NOT NULL REFERENCES employee(employee_id),
                  vacancies_name VARCHAR(255) NOT NULL,
                  salary_from INT,
                  url TEXT)
            """)
        self.conn.commit()

    def save_database_employee(self, vacancies):
        # запись информации о компаниях в базу данных
        employee = {}
        for vacancie in vacancies:
            print(vacancie)
            if vacancie['employer_id'] not in employee:
                employee[vacancie['employer_id']] = vacancie['employer_name']
        with self.conn:
            with self.conn.cursor() as cur:
                for employee_id, employee_name in employee.items():
                    cur.execute("INSERT INTO employee VALUES (%s, %s)", (
                        employee_id,
                        employee_name,
                    ))
                    cur.execute("SELECT * FROM employee")

                datas = cur.fetchall()
                for data in datas:
                    print(data)


    def save_database_vacancies(self, vacancies):
        # запись информации о вакансиях в базу данных
        with self.conn:
            with self.conn.cursor() as cur:
                for i in vacancies:
                    vacancies_id = i['vacancy_id']
                    employee_id = i['employer_id'],
                    vacancies_name = i['title'],
                    salary_from = i['salary_from'],
                    url = f"https://hh.ru/vacancy/{i['vacancy_id']}",
                    cur.execute("INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s)", (
                        vacancies_id,
                        employee_id,
                        vacancies_name,
                        salary_from,
                        url,
                    ))
                    cur.execute("SELECT * FROM vacancies")

                datas = cur.fetchall()
                for data in datas:
                    print(data)

    def get_companies_and_vacancies_count(self):
        # получает список всех компаний и количество вакансий у каждой компании
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT employee.employee_name, COUNT(vacancies.vacancies_name) AS count_vacancies
                    FROM employee
                    JOIN vacancies USING(employee_id)
                    GROUP BY employee.employee_name
                    ORDER BY count_vacancies
                """)

                data = cur.fetchall()
                return data


    def get_all_vacancies(self):
        # получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT employee_name, salary_from, url
                    FROM vacancies
                    JOIN employee USING(employee_id)
                    ORDER BY employee_name, salary_from, url
                """)
                data = cur.fetchall()
                return data

    def get_avg_salary(self):
        # получает среднюю зарплату по вакансиям
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT vacancies_name, ROUND(AVG(salary_from),2) AS avg_salary
                    FROM vacancies
                    GROUP BY vacancies_name
                    ORDER BY avg_salary
                """)
                data = cur.fetchall()
                return data

    def get_vacancies_with_higher_salary(self):
        # получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT vacancies_name, ROUND(AVG(salary_from),2)
                    FROM vacancies
                    GROUP BY vacancies_name, salary_from
                    HAVING salary_from > ROUND(AVG(salary_from),2)
                    ORDER BY vacancies_name
                """)
                data = cur.fetchall()
                return data

    def get_vacancies_with_keyword(self):
        # получает список всех вакансий, в названии которых содержатся переданные в метод слова, например SQL
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT vacancies_name FROM vacancies
                    WHERE vacancies_name LIKE '%SQL%'
                    ORDER BY vacancies_name
                """)
                data = cur.fetchall()
                return data
