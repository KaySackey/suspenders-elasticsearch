"""
Provides integration with Django
"""
import django.apps
from django.utils.module_loading import autodiscover_modules


class AppConfig(django.apps.AppConfig):
    name = "suspenders.app"
    label = "suspenders"
    verbose_name = "Suspenders ElasticSearch"

    def ready(self):
        super().ready()
        autodiscover_modules("suspenders_search")
