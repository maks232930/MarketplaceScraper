import csv
import re
from string import ascii_letters
from urllib.parse import urlparse, parse_qs

import requests

from src.yandex_market.config_product import BREAD_CRUMBS


def save_data_to_csv(filename, data):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)


def extract_product_links(response_json):
    product_links = {}
    product_show_place = response_json['collections']['productShowPlace'].values()

    for item in product_show_place:
        product_id = item['productId']
        url = item['urls']['direct']
        product_links[product_id] = f'https:{url}'

    product_links_offer = {}
    product_show_place = response_json['collections']['offerShowPlace']

    for value in product_show_place.values():
        urls = value.get('urls')
        if urls:
            url = urls.get('direct', '')
            if url.startswith('https://market.yandex.ru/product'):
                product_id = int(re.findall(r'\d{2,}', url)[0])
                product_links_offer[product_id] = url

    return product_links, product_links_offer


def extract_prices(response_json):
    prices = {}
    offer_data = response_json['collections']['offer'].values()

    for item in offer_data:
        product_id = item.get('productId')
        if product_id:
            price = item['price']['value']
            prices[product_id] = price

    return prices


def extract_is_resales(response_json):
    resales = {}
    resales_specs = {}
    offer_data = response_json['collections']['offer'].values()

    for item in offer_data:
        product_id = item.get('productId')
        if product_id:
            resale = item['isResale']
            resales[product_id] = resale

            resale_specs = item.get('resaleSpecs')
            if resale_specs:
                resales_specs[product_id] = resale_specs['condition']['value']

    return resales, resales_specs


def get_product_link(product, products_links, product_links_offer, resales, resales_specs):
    product_id = product['id']
    link = product_links_offer.get(product['id'], products_links[product['id']]).replace(',', '')

    if resales[product['id']]:
        link += '&resale_goods=resale_resale'

        if resales_specs.get(product_id):
            link += f'&resale_goods_condition={resales_specs[product_id]}'

    return link


def get_category(base_url, headers, params):
    result_category = []

    response = requests.post(base_url, headers=headers, json=params)

    for _ in range(1, 15):
        response_json = response.json()
        products = response_json['collections']['product']
        products_links, product_links_offer = extract_product_links(response_json)
        prices = extract_prices(response_json)
        resales, resales_specs = extract_is_resales(response_json)

        for product in products.values():
            if product['categoryIds'][0] != 91491:
                continue

            link = get_product_link(product, products_links, product_links_offer, resales, resales_specs)

            result_category.append([
                'https://market.yandex.ru/catalog--smartfony/61808/list',
                product['titles']['raw'],
                prices[product['id']],
                link
            ])

        params['params'][0]['page'] += 1
        response = requests.post(base_url, headers=headers, json=params)

    return result_category


def get_brand_name(title):
    brand_name = ''

    for item in title.split():
        if item[0] in ascii_letters:
            brand_name = item
            break

    return brand_name


def get_stock(response_json):
    try:
        stock = \
            response_json['scaffold']['bottomView']['divData']['states'][0]['div']['bottomItemsRef'][0]['custom_props'][
                'params'].get('availableCount', 0)
        return stock
    except KeyError:
        return 0


def get_price(response_json):
    try:
        price = response_json['scaffold']['wishButtonParams']['price'].get('value', '')
        return price
    except KeyError:
        return ''


def get_params_for_request(url):
    parsed_url = urlparse(url)

    query_parameters = parse_qs(parsed_url.query)
    product_id = re.findall(r'\d{2,}', url)[0]

    all_params = {
        'productId': product_id,
        'skuId': query_parameters.get('sku', [None])[0],
        'offerId': query_parameters.get('offerid', [None])[0],
        'resale_goods': query_parameters.get('resale_goods', [None])[0],
        'resale_goods_condition': query_parameters.get('resale_goods_condition', [None])[0]
    }

    params = {key: value for key, value in all_params.items() if value is not None}
    return params


def get_rating_and_reviews_count(response_json):
    rating = 0
    reviews_count = 0

    for item in response_json['shared']['analytics'].values():
        if 'score' in item and 'reviewsCount' in item:
            reviews_count = item['reviewsCount']
            rating = round(item['score'], 1)
            break
    return rating, reviews_count


def get_products(products, base_url, headers, json_data):
    result_products = []

    for product in products:
        params = get_params_for_request(product[3])

        response = requests.post(
            base_url,
            params=params,
            headers=headers,
            json=json_data,
        )

        response_json = response.json()

        stock = get_stock(response_json)
        title = response_json['scaffold']['title']
        price = get_price(response_json)
        link = product[3]
        brand_name = get_brand_name(title)
        rating, reviews_count = get_rating_and_reviews_count(response_json)

        result_products.append([
            title,
            price,
            link,
            stock,
            f'{BREAD_CRUMBS}>{brand_name}',
            rating,
            reviews_count,
        ])

    return result_products
