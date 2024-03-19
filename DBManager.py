import psycopg2


class DBManager:
    """Класс, который подключается к БД PostgreSQL."""
    def __init__(self, params):
        self.conn = psycopg2.connect(dbname='hhru', **params)
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        """
        получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        """
        self.cur.execute(f"SELECT company_name, open_vacancies FROM employers")
        return self.cur.fetchall()

    def get_all_vacancies(self):
        """
        получает список всех вакансий с указанием названия компании,названия вакансии и зарплаты и ссылки на вакансию.
        """
        self.cur.execute(f"select employers.company_name, vacancies.vacancy_name, vacancies.salary_from, "
                         f"vacancies.vacancy_url from vacancies join employers using(employer_id)")
        return self.cur.fetchall()

    def get_avg_salary(self):
        """
        получает среднюю зарплату по вакансиям.
        """
        self.cur.execute(f"select avg(salary_from) from vacancies")
        return self.cur.fetchall()

    def get_vacancies_with_higher_salary(self):
        """
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        """
        self.cur.execute(f"select vacancy_name, salary_from from vacancies group by vacancy_name, "
                         f"salary_from having salary_from > (select avg(salary_from) from vacancies)")
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self):
        """
        получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python.
        """
        self.cur.execute(f"select * from vacancies where vacancy_name LIKE 'О%';")
        return self.cur.fetchall()