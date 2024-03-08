from typing import Any
import requests
import psycopg2


def get_hhru_data(employer_ids: list[str]) -> list[dict[str, Any]]:
    """Получение данных о работодателях и вакансиях с помощью API HH.ru."""

    data = []
    for employer_id in employer_ids:
        response_employer = requests.get('https://api.hh.ru/employers/' + employer_id)
        employer_data = response_employer.json()

        vacancy_data = []
        response_vacancy = requests.get('https://api.hh.ru/vacancies?employer_id=' + employer_id)
        response_text_vac = response_vacancy.json()

        vacancy_data.extend(response_text_vac['items'])

        data.append({
            'employers': employer_data,
            'vacancies': vacancy_data
        })

    return data


def create_database(database_name: str, params: dict):
    """Создание базы данных и таблиц для сохранения данных"""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE employers (
                employer_id SERIAL PRIMARY KEY,
                company_name VARCHAR(500) NOT NULL,
                open_vacancies INTEGER,
                employer_url TEXT,
                description TEXT)
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                employer_id INT REFERENCES employers(employer_id),
                vacancy_name VARCHAR(500) NOT NULL,
                salary_from INTEGER,
                vacancy_url TEXT)
        """)

    conn.commit()
    conn.close()


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict):
    """Сохранение данных в базу данных."""

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for text in data:
            employer_data = text['employers']

            cur.execute(
                """
                INSERT INTO employers (company_name, open_vacancies, employer_url, description)
                VALUES (%s, %s, %s, %s)
                RETURNING employer_id
                """,
                (employer_data['name'], employer_data['open_vacancies'], employer_data['alternate_url'],
                 employer_data['description']))

            employer_id = cur.fetchone()[0]

            vacancies_data = text['vacancies']
            for vacancy in vacancies_data:
                if vacancy['salary'] is None:
                    cur.execute(
                        """
                        INSERT INTO vacancies (employer_id, vacancy_name, salary_from, vacancy_url)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (employer_id, vacancy['name'], 0,
                         vacancy['alternate_url'])
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO vacancies (employer_id, vacancy_name, salary_from, vacancy_url)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (employer_id, vacancy['name'], vacancy['salary']['from'],
                         vacancy['alternate_url']))

    conn.commit()
    conn.close()