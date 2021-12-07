import json
import re
from multiprocessing import Pool
from string import punctuation

import requests
from spacy import load
from spacy.lang.ru import RussianDefaults

VACANCY_URL = 'https://api.hh.ru/vacancies'
nlp = load('ru_core_news_md')
ADDITIONAL_PUNC = ['—', ':-', '-', *punctuation]
STOP_WORDS = RussianDefaults.stop_words


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def get_vacancies(page):
    params = {
        'text': 'NAME:Java',
        'page': page,
        'per_page': 100
    }
    req = requests.get(VACANCY_URL, params)
    req.close()
    return json.loads(req.content.decode())["items"]


def get_vac(vac_id):
    req = requests.get(f"{VACANCY_URL}/{vac_id}")
    req.close()
    return json.loads(req.content.decode())


def get_vacs(max_page):
    vacancies = []
    for page in range(max_page):
        vacancies.extend(get_vacancies(page))
    return vacancies


def prepare_vacancy_description(description):
    description = description.replace("quot", "")
    description = re.sub("\s+", " ", re.sub("\d+", "", description))
    description = re.sub("\s+", " ", re.sub("<[^>]*>", "", description))
    tokens = [token.lemma_ for token in nlp(description)]

    tokens1 = []
    for token in tokens:
        if token not in STOP_WORDS and token not in ADDITIONAL_PUNC:
            tokens1.append(re.sub("\W", "", token))

    return [token for token in tokens1 if len(token) > 1]


def get_prepared_all_vacancies_from_hh(vacancy_ids):
    results = []
    for vac_id in vacancy_ids:
        desc = get_vac(vac_id)['description']
        prepared = prepare_vacancy_description(desc)
        results.append(prepared)
    return results


if __name__ == '__main__':
    vacs = get_vacs(1)
    vac_ids = [vac['id'] for vac in vacs]
    vac_ids_parts = list(split(vac_ids, 5))
    res = []
    with Pool(5) as pool:
        print(pool.map(get_prepared_all_vacancies_from_hh, vac_ids_parts))
