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
    """Filter and rename nutrition elements keys, removing elements with zero or null amount_per_serving."""
    if not value:
        return []

    filtered_elements = []
    for elem in value:
        amount = elem.get('perServing')
        # Skip elements with null, None, 0, or empty string as perServing value
        if amount is None or amount == 0 or amount == '' or amount == '0':
            continue

        filtered_elements.append({
            'key': elem.get('key'),
            'name': elem.get('name'),
            'unit_of_measure': elem.get('uom'),
            'amount_per_serving': amount,
            'recommended_daily_value': elem.get('fullDvp')
        })

    return filtered_elements


def process_related_products(value):
    """Extract slug from each dictionary"""
    return [obj.get('slug') for obj in value] if value else []


def process_amazon_product_id(value):
    """Remove brackets from Amazon product ID string."""
    if not value:
        return value

    if isinstance(value, str):
        # Remove square brackets if present
        return value.replace('[', '').replace(']', '')
    elif isinstance(value, list):
        # If it's a list, process the first item
        first_item = value[0] if value else ''
        return first_item.replace('[', '').replace(']', '')

    return value


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
    amazon_product_id = scrapy.Field(
        input_processor=process_amazon_product_id,
        output_processor=TakeFirst()
    )
    rank = scrapy.Field(output_processor=TakeFirst())
    is_available = scrapy.Field(output_processor=TakeFirst())
    category = scrapy.Field(output_processor=TakeFirst())
    category_2 = scrapy.Field(output_processor=TakeFirst())
    category_3 = scrapy.Field(output_processor=TakeFirst())
    # multiple values, do not take first
    diets = scrapy.Field(input_processor=process_diets)
    # multple values, do not take first
    ingredients = scrapy.Field()
    # multiple values, do not take first
    allergens = scrapy.Field()
    # multiple values, do not take first
    additives = scrapy.Field()
    # multiple values, do not take first
    certifications = scrapy.Field()
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
