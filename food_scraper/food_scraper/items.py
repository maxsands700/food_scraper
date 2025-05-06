import scrapy
from itemloaders.processors import TakeFirst


def process_date_opened(value):
    """Extract date from ISO timestamp (e.g., 'YYYY-MM-DD')."""
    if isinstance(value, str):
        return value.split('T')[0]
    elif isinstance(value, list):
        return value[0].split('T')[0]
    return value


def process_diets(value):
    """Extract diet names from list of diet objects."""
    return [obj.get('name') for obj in value] if value else []


def process_nutrition_elements(value):
    """Filter and rename nutrition elements keys."""
    if not value:
        return []
    return [
        {
            'key': elem.get('key'),
            'name': elem.get('name'),
            'unit_of_measure': elem.get('uom'),
            'amount_per_serving': elem.get('perServing'),
            'recommended_daily_value': elem.get('fullDvp')
        }
        for elem in value
    ]


def process_related_products(value):
    """Extract slug from each dictionary"""
    return [obj.get('slug') for obj in value] if value else []


class StoreItem(scrapy.Item):
    store_id = scrapy.Field(output_processor=TakeFirst())
    status = scrapy.Field(output_processor=TakeFirst())
    date_opened = scrapy.Field(
        input_processor=process_date_opened,
        output_processor=TakeFirst()
    )
    latitude = scrapy.Field(output_processor=TakeFirst())
    longitude = scrapy.Field(output_processor=TakeFirst())
    street = scrapy.Field(output_processor=TakeFirst())
    city = scrapy.Field(output_processor=TakeFirst())
    state = scrapy.Field(output_processor=TakeFirst())
    zip_code = scrapy.Field(output_processor=TakeFirst())
    postal_code = scrapy.Field(output_processor=TakeFirst())


class ProductItem(scrapy.Item):
    store_id = scrapy.Field(output_processor=TakeFirst())
    name = scrapy.Field(output_processor=TakeFirst())
    price = scrapy.Field(output_processor=TakeFirst())
    slug = scrapy.Field(output_processor=TakeFirst())
    brand = scrapy.Field(output_processor=TakeFirst())
    asin = scrapy.Field(output_processor=TakeFirst())
    amazon_product_id = scrapy.Field(output_processor=TakeFirst())
    rank = scrapy.Field(output_processor=TakeFirst())
    is_available = scrapy.Field(output_processor=TakeFirst())
    detail_uri = scrapy.Field(output_processor=TakeFirst())
    category = scrapy.Field(output_processor=TakeFirst())
    child_category = scrapy.Field(output_processor=TakeFirst())
    # multiple values, do not take first
    diets = scrapy.Field(input_processor=process_diets)
    # multple values, do not take first
    ingredients = scrapy.Field()
    nutrition_group = scrapy.Field(output_processor=TakeFirst())
    nutrition_label_format = scrapy.Field(output_processor=TakeFirst())
    # multple values, do not take first
    nutrition_elements = scrapy.Field(
        input_processor=process_nutrition_elements)
    serving_info = scrapy.Field(output_processor=TakeFirst())
    is_alcoholic = scrapy.Field(output_processor=TakeFirst())
    unit_of_measure = scrapy.Field(output_processor=TakeFirst())
    image = scrapy.Field(output_processor=TakeFirst())
    # multple values, do not take first
    related_products = scrapy.Field(input_processor=process_related_products)
