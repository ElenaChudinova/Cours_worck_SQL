from functions import HHru, Vacancy
from config import config
from db_manager import DBManager



def main():
    params = config()
    manager = DBManager(params, 'cours_worck_5')
    manager.create_table_employee()
    manager.create_table_vacancies()
    vacancies_json = []
    for employer_id in 3350812, 4362108, 3634424, 3565331, 54964, 2617, 39305, 87021, 5077410, 2343:
        hh = HHru(employer_id)
        hh.get_request()
        vacancies_json.extend(hh.get_formated_vacanies())

    manager.save_database_employee(vacancies_json)
    manager.save_database_vacancies(vacancies_json)


    while True:
        comand = input(
            "1 - Вывести список всех компаний и количество вакансий у каждой компании;\n"
            "2 - Вывести список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию;\n"
            "3 - Вывести список со средней зарплатой по вакансиям;\n"
            "4 - Вывести список всех вакансий, у которых зарплата выше средней по всем вакансиям;\n"
            "5 - Вывести список всех вакансий, в названии которых содержатся переданные в метод слова, например - python;\n"
            "0 - для выхода.\n"
            ">>>"
        )
        if comand.lower() == '0':
            break
        elif comand == "1":
            vacancies = manager.get_companies_and_vacancies_count()
        elif comand == "2":
            vacancies = manager.get_all_vacancies()
        elif comand == "3":
            vacancies = manager.get_avg_salary()
        elif comand == "4":
            vacancies = manager.get_vacancies_with_higher_salary()
        elif comand == "5":
            vacancies = manager.get_vacancies_with_keyword()



        for vacancy in vacancies:
            print(vacancy, end='\n')





if __name__ == "__main__":
    main()
