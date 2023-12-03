import psycopg2
import json
import os
import requests
from datetime import datetime
from configparser import ParsingError
from config import config

path_vacancies = './vacancies.json'

path = os.path.join(os.path.dirname(__file__), path_vacancies)
params = config()


class HHru:
    """
    Создаем класс, который умеет подключаться к API HeadHunter
    и получать вакансии.
    """

    def __init__(self, employer_id):
        self.url = 'https://api.hh.ru/vacancies'
        self.params = {
            "area": 113,
            "per_page": 10,
            "page": None,
            "employer_id": employer_id,
            "archive": False,
            "currency": "RUR",
        }
        self.headers = {
            "User-Agent": "Vacancies_ParserApp/1.0"
        }

    def get_request(self):
        response = requests.get(self.url, headers=self.headers, params=self.params)
        if response.status_code != 200:
            raise ParsingError(f"Ошибка получения вакансий! Статус: {response.status_code}")
        return response.json()["items"]

    def get_formated_vacanies(self):
        # Получаем отформатированные вакансии под единый формат
        formated_vacanies = []
        for vacanies in self.get_request():
            published_date_hh = datetime.strptime(vacanies.get('published_at'), "%Y-%m-%dT%H:%M:%S%z")
            vacancy_title = vacanies.get('name')
            vacancy_employer_id = vacanies.get('employer')['id']
            vacancy_employer_name = vacanies.get('employer')['name']
            vacancy_area = vacanies.get('area')['name']
            vacancy_url = f"https://hh.ru/vacancy/{vacanies.get('id')}"
            salary = vacanies.get('salary')
            if not salary:
                salary_from = 0
                salary_to = 0
                currency = ''
            else:
                salary_from = salary.get('from')
                salary_to = salary.get('to')
                if not salary_from:
                    salary_from = salary_to
                if not salary_to:
                    salary_to = salary_from
                currency = vacanies.get('salary')['currency']
            experience = vacanies.get('experience')['name']
            requirements = (vacanies.get('snippet')['requirement'])
            vacancy_id = vacanies.get('id')
            vacancy_date = published_date_hh.strftime("%d.%m.%Y")
            if requirements:
                requirements = requirements.strip().replace('<highlighttext>', '').replace('</highlighttext>', '')

            vacanies_info = {
                'employer_id': vacancy_employer_id,
                'vacancy_id': vacancy_id,
                'employer_name': vacancy_employer_name,
                'title': vacancy_title,
                'area': vacancy_area,
                'url': vacancy_url,
                'salary_from': salary_from,
                'salary_to': salary_to,
                'currency': currency,
                'experience': experience,
                'requirements': requirements,
                'date': vacancy_date,
            }

            formated_vacanies.append(vacanies_info)

        return formated_vacanies


class Vacancy:
    """
    Создаем класс для работы с вакансиями. В этом классе определяем нужные нам атрибуты:

    - название вакансии;
    - ссылка на вакансию;
    - зарплата;
    - опыт и требования;
    - город;
    - дата публикации.
    Класс поддерживает методы сравнения вакансий между собой по зарплате.
    """

    def __init__(self, keyword):
        self.employer_id = ["employer_id"]
        self.vacancy_id = ["vacancy_id"]
        self.employer_name = ["employer_name"]
        self.title = ["title"]
        self.salary_from = ["salary_from"]
        self.salary_to = ["salary_to"]
        self.url = ["url"]
        self.requirements = ["requirements"]
        self.area = ["area"]
        self.date = ["date"]
        self.currency = ["currency"]
        self.path_vacancies = f"{keyword.title()}.json()"

    def __str__(self):
        return f"""
        Компания: \"{self.employer_name}"
        Вакансия: \"{self.title}"
        Зарплата: \"от {self.salary_from} до {self.salary_to} {self.currency}"
        Ссылка: \"{self.url}"
        Опыт и обязанности: \"{self.requirements}"
        Город: \"{self.area}"
        Дата публикации: \"{self.date}"
        """

    def __gt__(self, other):
        return self.salary_from > other.salary_from

    def __lt__(self, other):
        if other.salary_from is None:
            # e.g., 10 < None
            return False
        if self.salary_from is None:
            # e.g., None < 10
            return True

        return self.salary_from < other.salary_from

    def insert_vacancies_and_employer(self, vacancies_json):
        with open(self.path_vacancies, "w", encoding='UTF-8') as file:
            json.dump(vacancies_json, file, skipkeys=False, ensure_ascii=False, default=str, indent=4)

    def select_vacancies_and_employer(self):
        with open(self.path_vacancies, "r", encoding='UTF-8') as file:
            vacancies = json.load(file)
        return [Vacancy(x) for x in vacancies]


class DBManager:

    def __init__(self, params, database_name):
        # Инициализатор класса для подключения к БД Postgres и автосохранение новой информации
        self.conn = psycopg2.connect(dbname='postgres', **params)
        self.cur = self.conn.cursor()
        self.cur.execute(f'DROP DATABASE {database_name}')
        self.cur.execute(f'CREATE DATABASE {database_name}')

        self.conn.close()
        self.conn = psycopg2.connect(dbname=database_name, **params)

    def create_table_employee(self):
        with self.conn.cursor() as cur:
            cur.execute('''
            CREATE TABLE employee(
            employee_id INT PRIMARY KEY NOT NULL,
            employee_name VARCHAR(20) NOT NULL,
            area VARCHAR(20) NOT NULL,
            publish_date DATE NOT NULL;
            )
        ''')

    def create_table_vacancies(self):
        with self.conn.cursor() as cur:
            cur.execute('''
                  CREATE TABLE vacancies(
                  vacancies_id INT PRIMARY KEY NOT NULL,
                  employee_id INT NOT NULL REFERENCES employee(employee_id),
                  vacancies_name VARCHAR(20) NOT NULL,
                  salary_from INT,
                  salary_to INT,
                  currency VARCHAR(10),
                  requirements TEXT NOT NULL,
                  url TEXT;
                  )
            ''')
        self.conn.commit()
        self.conn.close()

    def save_database_employee(self):
        try:

            with self.conn:
                with self.conn.cursor() as cur:
                    with open(path, 'r', encoding='UTF-8') as file:
                        reader = json.DictReader(file)
                        for i in reader:
                            employee_id = i['employee_id'],
                            employee_name = i['employee_name'],
                            publish_date = i['date'],
                            area = i['area']
                            cur.execute("INSERT INTO employee VALUES (%s, %s, %s, %s)", (
                                employee_id,
                                employee_name,
                                publish_date,
                                area
                            ))
                            cur.execute("SELECT * FROM employee")

                            datas = cur.fetchall()
                            for data in datas:
                                print(data)

        finally:
            self.conn.close()

    def save_database_vacancies(self):
        try:

            with self.conn:
                with self.conn.cursor() as cur:
                    with open(path, 'r', encoding='UTF-8') as file:
                        reader = json.DictReader(file)
                        for i in reader:
                            vacancies_id = i['vacancies_id']
                            employee_id = i['employee_id'],
                            vacancies_name = i['title'],
                            salary_from = i['salary_from'],
                            salary_to = i['salary_to'],
                            currency = i['currency'],
                            requirements = i['requirements'],
                            url = f"https://hh.ru/vacancy/{i['items'][0]['id']}",
                            cur.execute("INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (
                                vacancies_id,
                                employee_id,
                                vacancies_name,
                                salary_from,
                                salary_to,
                                currency,
                                requirements,
                                url,
                            ))
                            cur.execute("SELECT * FROM vacancies")

                            datas = cur.fetchall()
                            for data in datas:
                                print(data)

        finally:
            self.conn.commit()
            self.conn.close()

    def get_companies_and_vacancies_count(self):
        # получает список всех компаний и количество вакансий у каждой компании
        with self.conn:
            self.cur.execute(
                "SELECT DISTINCT employee.employee_name, COUNT(vacancies.vacancies_name) AS count_vacancies"
                "FROM employee"
                "JOIN vacancies USING(employee_id)"
                "GROUP BY employee.employee_name"
                "ORDER BY count_vacancies;"
            )
        data = self.cur.fetchall()
        return data

        self.conn.commit()
        self.conn.close()

    def get_all_vacancies(self):
        # получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
        with self.conn:
            self.cur.execute(
                "SELECT employee_name, name, salary_from, salary_to, url"
                "FROM vacancies"
                "JOIN employee USING(employee_id)"
                "ORDER BY employee_name, name, salary_from, salary_to, url"
            )
        data = self.cur.fetchall()
        return data

    def get_avg_salary(self):
        # получает среднюю зарплату по вакансиям
        with self.conn:
            self.cur.execute(
                "SELECT name, AVG(salary_from, salary_to) AS avg_salary"
                "FROM vacancies "
                "GROUP BY name"
                "ORDER BY avg_salary;"
            )
        data = self.cur.fetchall()
        return data

    def get_vacancies_with_higher_salary(self):
        # получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        with self.conn:
            self.cur.execute(
                "SELECT name, AVG(salary_from, salary_to) AS avg_salary"
                "FROM vacancies "
                "GROUP BY name"
                "ORDER BY avg_salary;"
            )
        data = self.cur.fetchall()
        return data

    def get_vacancies_with_keyword(self):
        # получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python
        with self.conn:
            self.cur.execute(
                "SELECT name FROM vacancies"
                "WHERE name LIKE '%python%'"
                "ORDER BY name;"
            )
        data = self.cur.fetchall()
        return data
