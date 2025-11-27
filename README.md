# Suspenders-Elasticsearch

version: 0.1.0 (alpha)

# What is Suspenders?

A Django-friendly Python library for querying Elasticsearch and returning actual model instances.

# Why Suspenders?

Originally written for ElasticSearch < 1.0 and updated throughout the 1.0 series
At the time PyES was available, but its API was complex.

I wanted a fluent API and to make the most common functions of accessing ElasticSearch easy to do from Django.
Suspenders easily hooks up to your Django app and updates/deletes corresponding elasticsearch records whenever the object is changed in Django.
Additionally, it provides a query/filter API that makes basic search extremely simple and will reify the returned documents into a Django object.

It does not support the full ES API (a notable example being the Geo API), but if your needs are fairly simply like mine, then you'll find this useful.

### Stability

I've been using it in production since 2012. It's made it through a few major upgrades of both Django and ElasticSearch since then.
It's been stable for a few years now running a production site that sees about 9M queries per day, all on the search endpoint.
I wanted to share it with the community, and I hope it helps someone else.

## What does it do well?

- Easy to use query API
- Automatically updates elasticsearch records when a model is changed in Django
- Supports multi-index mappings
- Unlike other libraries:
  - it doesn't require you to write any custom mapping code
  - it doesn't require you to write custom save/delete hooks
  - it returns real Django objects, and if you access a field that wasn't loaded, it will lazy-load it for you
  - it has basic checks for invalid queries and invalid combinations of filters

## What does it not do well?
- It doesn't support the full ES API.
- It doesn't support aggregations except for terms aggregations.


# Installation

- Add it to your apps section of django as "suspenders".
- Create a "suspenders_search.py" file in your app directory for any models you want to index.
- This file will all automatically be imported.

In your settings.py
```python 

INSTALLED_APPS = [
    "myapp",
    "suspenders"
]

# This will prefix all index names with this string. Useful for multi-tenancy.
ELASTIC_SEARCH_PREFIX = "" 

# ElasticSearch settings
# This is a list of tuples of (host, port).
ELASTIC_SEARCH = {"server": [
    ("localhost", "9200"),
    ("otherhost", "9200"),],
    "timeout": 2.0
}

```
## Example suspenders_search.py file:
```python
from myapp.models import MyModel
from common.lib.suspenders.app.models import SuspendersModel
from common.lib.suspenders.app.sites import register
from common.lib.suspenders.mappings.fields import (
    BooleanField,
    DateField,
    DoubleField,
    IntegerField,
    KeywordField,
    TextField,
)

class MyModelMap(SuspendersModel):
    class Meta:
        model = MyModel

    field = KeywordField()
    another_field = IntegerField()
    
    
    def prepare_another_field(self, obj):
        # Not necessary, but shows how to customize field preparation
        return obj.another_field * 30

# Register the model with suspenders
# This will let it automatically save and delete records from elasticsearch
register(MyModel, MyModelMap)
```
## Create the indexes

```bash
# First time
python manage.py rebuild_index MyModel

# Populate after making changes to the mapping
python manage.py populate_indexes MyModel
```


## Querying
```python  
qs = MyModel.search.query_set()

qs = qs.filter(field='value').sorted_by('-date_created').paginate(page=1, page_size=10)

results = qs.results(search_type="query_then_fetch")


for result in results:
    print(result.field)

for result in results.as_dict():
    print(result['field'])
```

Additional functionality includes:
- bulk updates and deletes
- aggregation support
- 

## Class Structure

IndexedItem:
- Base class for indexed items. Inherit from this class to make your model indexable. 
- Gives you the helper functions in case you want to manually update/delete records from elasticsearch.
  - add_to_index
  - remove_from_index
It adds additoinal functionality like
  - checking if refresh_from_db was called on the model instance if DEBUG_REFRESHES is true 

SuspendersQuerySet
- A QuerySet-like object that allows you to build up a query using filter, order_by, limit, and offset.

ResultSet
- A list-like object that wraps the results of a query.

Various filters are supported:
- TermFilter
- TermsFilter
- RangeFilter

Only one aggregation is supported:
- TermsAggregation

Usually none of these need to be used directly, and are exposed as via calling .filter() and .aggregation() on a query set.

## Advanced: Multi-Index Mapping

Normally, you can only map a single index to a model. However, you can use the `indexes` attribute to search multiple indexes at the same time with one query.

For example, you might have a model that is indexed in two different indexes: `album` and `video`.

```python3
class MultiIndexSearch(BaseMap):
    doc_type = None
    indexes = ["album", "video"]

    mappers = {
        "manga": AlbumMap(),
        "video": VideoMap(),
    }

    def execute_callback(self, result_set):
        # The results will be from two different indexes
        # So we disambiguate them here
        def query_subclass_type_from_fields(base, parent=None):
            doc_type = base.get("document_type", base.get("_type"))

            cls = self.mappers[doc_type]
            return cls.type_from_fields(base)

        result_set.type_from_fields = query_subclass_type_from_fields
        return result_set
```

## Caveats
- Unwrapping of nested objects that also adjust the Django model metaclass is not readily supported (e.g. Django-MPTT).