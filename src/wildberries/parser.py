import aiofiles
import aiohttp
from aiocsv import AsyncWriter

from src.wildberries.config import HEADERS


async def get_data(url: str, session) -> list:
    async with session.get(url, headers=HEADERS) as response:
        data = await response.json()
        products = data.get('data', {}).get('products', [])
        return products


async def save_data_to_csv(data, filename):
    async with aiofiles.open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = AsyncWriter(file)
        await writer.writerows(data)


async def get_categories(data):
    result = []

    for index, item in enumerate(data, start=1):
        # page_number = (index - 1) // 100 + 1
        # f'https://www.wildberries.ru/catalog/elektronika/smartfony-i-telefony/vse-smartfony?sort=popular&page={page_number}',
        result.append([
            f'https://www.wildberries.ru/catalog/elektronika/smartfony-i-telefony/vse-smartfony?sort=popular',
            item['name'],
            str(item['salePriceU'])[:-2] or str(item['priceU'])[:-2],
            f'https://www.wildberries.ru/catalog/{item["id"]}/detail.aspx'
        ])
    return result


async def get_products(data):
    result = []

    async with aiohttp.ClientSession() as session:
        for item in data:
            product_id = item[-1].split('/')[-2]
            url = f'https://card.wb.ru/cards/detail?appType=1&curr=rub&dest=-1257786&regions=80,38,83,4,64,33,68,70,30,40,86,75,69,1,31,66,110,48,22,71,114&spp=31&nm={product_id}'
            response = await get_data(url=url, session=session)
            product = response[-1]
            result.append([
                product['name'],
                str(product['salePriceU'])[:-2] or str(product['priceU'])[:-2],
                item[-1],
                product['sizes'][0]['stocks'][0]['qty'],
                f'Главная/Электроника/Смартфоны/{product["brand"]}',
                product['reviewRating'],
                product['feedbacks'],
            ])

    return result
