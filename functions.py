import requests
from datetime import datetime
from configparser import ParsingError


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

    def __init__(self, vacancy):
        self.employer_id = vacancy["employer_id"]
        self.vacancy_id = vacancy["vacancy_id"]
        self.employer_name = vacancy["employer_name"]
        self.title = vacancy["title"]
        self.salary_from = vacancy["salary_from"]
        self.salary_to = vacancy["salary_to"]
        self.url = vacancy["url"]
        self.requirements = vacancy["requirements"]
        self.area = vacancy["area"]
        self.date = vacancy["date"]
        self.currency = vacancy["currency"]

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
