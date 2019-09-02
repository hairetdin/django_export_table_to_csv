# -*- coding: utf-8 -*-

import os
import csv
import sys
import logging

from django.db.models.constants import LOOKUP_SEP
from django.core.exceptions import FieldDoesNotExist
from django.core.management.base import BaseCommand
from django.core.management import CommandError
from django.apps import apps
from django.utils.encoding import force_text


LOG = logging.getLogger(__name__)

DATA_ROOT = '/tmp/'
APP_LABEL= 'auth'


def get_verbose_name(model_name, lookup, app_label=APP_LABEL):
    model = apps.get_model(app_label, model_name)
    name_list = []
    next_iter = len(lookup.split(LOOKUP_SEP))
    for part in lookup.split(LOOKUP_SEP):
        try:
            f = model._meta.get_field(part)
        except FieldDoesNotExist:
            # check if field is related
            for f in model._meta.related_objects:
                if f.get_accessor_name() == part:
                    break
            else:
                raise ValueError("Invalid lookup string")
        if f.is_relation and next_iter:
            next_iter -= 1
            model = f.related_model
            try:
                verbose_name = f.verbose_name
            except:
                verbose_name = model._meta.verbose_name
            name_list.append(force_text(verbose_name))
            if next_iter:
                continue
        else:
            verbose_name = f.verbose_name
            name_list.append(force_text(verbose_name))

    return u' - '.join(name_list)


def get_model_fields_dict(model_name, app_label=APP_LABEL):
    try:
        model = apps.get_model(app_label, model_name)
    except LookupError:
        model = None
        return u"Table not found"
    # model_fields = [field.name for field in model._meta.get_fields()]
    fields_dict = {}
    for field in model._meta.get_fields():
        field_verbose_name = get_verbose_name(model_name, field.name, app_label=app_label)
        fields_dict[field.name]= field_verbose_name
    return fields_dict


def create_csv_data(model_name, app_label=APP_LABEL, field_list=None, fields_caption = None):
    out_file_name = os.path.join(DATA_ROOT, "unload-{}.csv".format(model_name))
    # output = StringIO()
    model = apps.get_model(app_label, model_name)
    if field_list is None:
        fields_dict = get_model_fields_dict(model_name, app_label=app_label)
        field_list = [k for k in fields_dict]
        fields_caption = [fields_dict[k] for k in fields_dict]

    # save table to file
    write_type = 'w' if sys.version_info[0]==3 else 'wb'
    with open(out_file_name, write_type) as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        if fields_caption:
            writer.writerow(fields_caption)
        writer.writerow(field_list)

        qs_ids = model.objects.all().values_list('id', flat=True)

        for id in qs_ids.iterator():
            row = model.objects.filter(id=id).values_list(*field_list).first()
            writer.writerow(row)

    return out_file_name


class Command(BaseCommand):
    help = 'Export table data to csv file'

    def add_arguments(self, parser):
        parser.add_argument('--table', action='store', dest='table', default=None)
        parser.add_argument('--app', action='store', dest='app', default=None)

    def handle(self, table, app, **options):
        model_name = table if table else 'user'
        if not model_name:
            raise CommandError("--table is required")
        app_label = app if app else APP_LABEL

        LOG.info(u'Unload table {} started'.format(model_name))
        output_file_name = create_csv_data(model_name, app_label=app_label)
        LOG.info(u'Export table {} to csv complete. Created file: {}'.format(model_name, output_file_name))
        print(u'Export table {} to csv complete. Created file: {}'.format(model_name, output_file_name))
