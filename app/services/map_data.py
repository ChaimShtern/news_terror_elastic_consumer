import math

from app.db.elastic_client import elastic_search_client
from app.repository.elastic_repo import insert_bulk_data


def sanitize_data(data):
    # מחליף כל מופע של NaN ב-null
    for key, value in data.items():
        if isinstance(value, float) and (math.isnan(value) or value is None):
            data[key] = None  # או לחלופין, אפשר להמיר לערך אחר כמו "" או 0
    return data


def map_to_elastic_format(data):
    converted_data = []
    for i in data:
        sanitize_data(i)
        # מבנה הנתונים הרצוי
        converted_data.append({
            'city': i.get('city'),
            'country': i.get('country'),
            'region': None,
            'date': i.get('date'),
            'title': i.get('title'),
            'new': i.get('new')
        })
    insert_bulk_data(elastic_search_client(), 'terror', converted_data)
