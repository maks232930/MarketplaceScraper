import asyncio
import csv
import re
from string import ascii_letters
from urllib.parse import urlparse, parse_qs

import aiohttp

from src.yandex_market.config_product import BREAD_CRUMBS, MAX_CONCURRENT_REQUESTS


async def fetch_data(session, url, **kwargs):
    """
    Выполняет HTTP-запрос и возвращает JSON-ответ.

    :param session: aiohttp.ClientSession
    :param url: URL-адрес для запроса
    :param kwargs: Дополнительные аргументы для запроса
    :return: JSON-ответ
        """
    async with session.post(url, **kwargs) as response:
        response_json = await response.json()
        return response_json


async def save_data_to_csv(filename, data):
    """
    Сохраняет данные в CSV-файл.

    :param filename: Имя CSV-файла
    :param data: Данные для сохранения
    """
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)


async def extract_product_links(response_json):
    """
    Извлекает ссылки на продукты из JSON-ответа.

    :param response_json: JSON-ответ
    :return: Словари ссылок на продукты
    """
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


async def extract_prices_titles_is_resales(response_json):
    """
    Извлекает цены, названия, флаги перепродажи и спецификации перепродажи из JSON-ответа.

    :param response_json: JSON-ответ
    :return: Словари цен, названий, флагов перепродажи и спецификаций перепродажи
    """
    prices = {}
    titles = {}
    resales = {}
    resales_specs = {}

    offer_data = response_json['collections']['offer'].values()

    for item in offer_data:
        product_id = item.get('productId')
        if product_id:
            price = item['price']['value']
            prices[product_id] = price

            title = item['titlesWithoutVendor']['raw']
            titles[product_id] = title

            resale = item['isResale']
            resales[product_id] = resale

            resale_specs = item.get('resaleSpecs')
            if resale_specs:
                resales_specs[product_id] = resale_specs['condition']['value']

    return prices, titles, resales, resales_specs


async def get_product_link(product, products_links, product_links_offer, resales, resales_specs):
    """
    Получает ссылку на продукт с параметрами перепродажи (если есть).

    :param product: Данные о продукте
    :param products_links: Словарь ссылок на продукты
    :param product_links_offer: Словарь ссылок на продукты (предложения)
    :param resales: Словарь флагов перепродажи
    :param resales_specs: Словарь спецификаций перепродажи
    :return: Ссылка на продукт с параметрами
    """
    product_id = product['id']
    link = product_links_offer.get(product['id'], products_links[product['id']]).replace(',', '')

    if resales[product['id']]:
        link += '&resale_goods=resale_resale'

        if resales_specs.get(product_id):
            link += f'&resale_goods_condition={resales_specs[product_id]}'

    return link


async def get_category(base_url, headers, params):
    """
    Получает данные о продуктах в категории.

    :param base_url: Базовый URL для API категории
    :param headers: Заголовки для HTTP-запроса
    :param params: Параметры для запроса API
    :return: Список данных о продуктах
    """
    result_category = []
    async with aiohttp.ClientSession() as session:
        response_json = await fetch_data(session, base_url, headers=headers, json=params)

        for _ in range(1, 15):
            products = response_json['collections']['product']
            products_links, product_links_offer = await extract_product_links(response_json)
            prices, titles, resales, resales_specs = await extract_prices_titles_is_resales(response_json)

            for product in products.values():
                if product['categoryIds'][0] != 91491:
                    continue

                link = await get_product_link(product, products_links, product_links_offer, resales, resales_specs)

                result_category.append([
                    'https://market.yandex.ru/catalog--smartfony/61808/list',
                    titles[product['id']],
                    prices[product['id']],
                    link
                ])

            params['params'][0]['page'] += 1
            response_json = await fetch_data(session, base_url, headers=headers, json=params)

    return result_category


#############################################################PRODUCT####################################################

async def get_brand_name(title):
    """
    Извлекает имя бренда из названия продукта.

    :param title: Название продукта
    :return: Имя бренда
    """
    brand_name = ''

    for item in title.split():
        if item[0] in ascii_letters:
            brand_name = item
            break

    return brand_name


async def get_stock(response_json):
    """
    Получает данные о доступном количестве товара на складе.

    :param response_json: JSON-ответ
    :return: Доступное количество товара на складе
    """
    try:
        stock = \
            response_json['scaffold']['bottomView']['divData']['states'][0]['div']['bottomItemsRef'][0]['custom_props'][
                'params'].get('availableCount', 0)
        return stock
    except KeyError:
        return 0


async def get_price(response_json):
    """
    Получает цену продукта.

    :param response_json: JSON-ответ
    :return: Цена продукта
    """
    try:
        price = response_json['scaffold']['wishButtonParams']['price'].get('value', '')
        return price
    except KeyError:
        return ''


async def get_params_for_request(url):
    """
    Извлекает параметры для выполнения запроса на основе URL.

    :param url: URL-адрес продукта
    :return: Параметры запроса
    """
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


async def get_rating_and_reviews_count(response_json):
    """
    Получает рейтинг и количество отзывов о продукте.

    :param response_json: JSON-ответ
    :return: Рейтинг и количество отзывов о продукте
    """
    rating = 0
    reviews_count = 0

    for item in response_json['shared']['analytics'].values():
        if 'score' in item and 'reviewsCount' in item:
            reviews_count = item['reviewsCount']
            rating = round(item['score'], 1)
            break
    return rating, reviews_count


async def get_title(response_json):
    """
    Получает название продукта из JSON-ответа.

    :param response_json: JSON-ответ
    :return: Название продукта
    """
    try:
        title = response_json['scaffold'].get('title')
        return title
    except KeyError:
        return ''


async def get_products(products, base_url, headers, json_data):
    """
    Получает данные о продуктах и обрабатывает их.

    :param products: Список продуктов для обработки
    :param base_url: Базовый URL для API
    :param headers: Заголовки для HTTP-запросов
    :param json_data: Данные JSON для запросов
    :return: Список обработанных данных о продуктах
    """
    result_products = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = []

        for product in products:
            params = await get_params_for_request(product[3])
            tasks.append(
                fetch_and_process_product(session, base_url, product, params, headers, json_data, result_products,
                                          semaphore)
            )

        await asyncio.gather(*tasks)

    return result_products


async def fetch_and_process_product(session, base_url, product, params, headers, json_data, result_products, semaphore):
    """
    Выполняет запрос на продукт и обрабатывает полученные данные.

    :param session: aiohttp.ClientSession
    :param base_url: Базовый URL для API
    :param product: Данные о продукте
    :param params: Параметры для запроса
    :param headers: Заголовки для HTTP-запроса
    :param json_data: Данные JSON для запроса
    :param result_products: Список для сохранения обработанных данных о продуктах
    :param semaphore: Семафор для ограничения параллельных запросов
    """
    async with semaphore:
        response_json = await fetch_data(session, base_url, params=params, headers=headers, json=json_data)

    stock = await get_stock(response_json)
    title = await get_title(response_json)
    price = await get_price(response_json)
    link = product[3]
    brand_name = await get_brand_name(title)
    rating, reviews_count = await get_rating_and_reviews_count(response_json)

    result_products.append([
        title,
        price,
        link,
        stock,
        f'{BREAD_CRUMBS}>{brand_name}',
        rating,
        reviews_count,
    ])
