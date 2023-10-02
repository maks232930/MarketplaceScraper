from src.ozon.parser import get_data, save_data_to_csv
from src.ozon.config import HEADERS, START_URL, product_csv_path, category_csv_path


def main():
    result_category, result_products = get_data(START_URL, HEADERS)

    save_data_to_csv(product_csv_path, result_products[:1000])
    save_data_to_csv(category_csv_path, result_category[:1000])


if __name__ == '__main__':
    main()
