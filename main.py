from functions import HHru, Vacancy


def main():
    vacancies_json = []
    keyword = 'SQL'
    for employer_id in 3350812, 4362108, 3634424, 3565331, 54964, 2617, 39305, 87021, 5077410, 2343:
        hh = HHru(employer_id)
        hh.get_request()
        vacancies_json.extend(hh.get_formated_vacanies())

    vacancy = Vacancy(keyword=keyword)
    vacancy.insert_vacancies_and_employer(vacancies_json)

    while True:
        comand = input(
            "1 - Вывести список вакансий;\n"
            "2 - Вывести список компаний;\n"
            "0 - для выхода.\n"
            ">>>"
        )
        if comand.lower() == '0':
            break
        elif comand == "1":
            vacancies = vacancy.select_vacancies_and_employer()
        # elif comand == "2":
        #     vacancies = vacancy.()

        for vacancy in vacancies:
            print(vacancy, end='\n')


if __name__ == "__main__":
    main()
