import aiohttp
import asyncio

from src.wildberries.config import category_csv_path, product_csv_path
from src.wildberries.parser import get_data, get_categories, save_data_to_csv, get_products


async def main():
    all_items = []

    async with aiohttp.ClientSession() as session:
        for i in range(1, 11):
            url = f'https://catalog.wb.ru/catalog/electronic22/catalog?appType=1&curr=rub&dest=-1257786&page={i}&regions=80,38,83,4,64,33,68,70,30,40,86,75,69,1,31,66,110,48,22,71,114&sort=popular&spp=31&subject=515'
            data = await get_data(url, session)
            all_items.extend(data)

    category = await get_categories(all_items)
    await save_data_to_csv(category, category_csv_path)

    products = await get_products(category)

    await save_data_to_csv(products, product_csv_path)


if __name__ == '__main__':
    asyncio.run(main())
