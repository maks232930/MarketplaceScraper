import csv
import json
from urllib.parse import urlparse, urlunparse

import requests

from src.ozon.config import BASE_URL, BREAD_CRUMBS


def save_data_to_csv(filename, data):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)


def get_stock(product):
    stock = 0

    if 'stockCountBigPromo' in product:
        stock = product['stockCountBigPromo']
    elif 'stockCount' in product:
        if product['stockCount'] == 5:
            stock = 'Больше 5'
        else:
            stock = product['stockCount']

    return stock


def get_link(product):
    url = f'https://www.ozon.ru{product["link"]}'
    parsed_url = urlparse(url)
    base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))

    return base_url


def get_rating(product):
    rating = product.get('rating', 0)
    if rating != 0:
        rating = round(rating, 1)

    return rating


def get_bread_crumbs(product):
    brand_name = product.get("brandName", None)
    if brand_name:
        return f'{BREAD_CRUMBS}>{brand_name}'
    else:
        return BREAD_CRUMBS


def get_data(start_url, headers):
    result_category = []
    result_products = []

    response = requests.get(start_url, headers=headers)

    for _ in range(36):
        response_json = response.json()
        next_page = response_json['nextPage']

        for value in response_json['trackingPayloads'].values():
            if 'product' in value[:10] or 'title' not in value:
                continue
            product = json.loads(value)

            if product['title'] in ['Перейти', 'Похожие']:
                continue

            result_category.append([
                'https://www.ozon.ru/category/smartfony-15502/',
                product['title'],
                product['finalPrice'],
                f'https://www.ozon.ru{product["link"]}'
            ])

            link = get_link(product)
            stock = get_stock(product)
            rating = get_rating(product)
            bread_crumbs = get_bread_crumbs(product)

            result_products.append([
                product['title'],
                product['finalPrice'],
                link,
                stock,
                bread_crumbs,
                rating,
                product.get("countItems", 0),
            ])

        response = requests.get(f'{BASE_URL}{next_page}', headers=headers)

    return result_category, result_products
