import json

from src.yandex_market.parser import get_category, save_data_to_csv, get_products
from src.yandex_market.config_category import (
    BASE_URL_CATEGORY,
    headers_category,
    category_csv_path,
    json_data_category
)
from src.yandex_market.config_product import (
    HEADERS_PRODUCT,
    BASE_URL_PRODUCT,
    json_data_product,
    product_csv_path
)


def main():
    result_category = get_category(BASE_URL_CATEGORY, headers_category, json_data_category)
    # with open('../../data/yandex_market/category.json', 'r') as file:
    #     result_category = json.load(file)
    #
    result_products = get_products(result_category, BASE_URL_PRODUCT, HEADERS_PRODUCT, json_data_product)
    save_data_to_csv(product_csv_path, result_products[:1000])
    save_data_to_csv(category_csv_path, result_category[:1000])


if __name__ == '__main__':
    main()
