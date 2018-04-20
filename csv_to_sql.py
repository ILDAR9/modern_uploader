import os, sys
from collections import OrderedDict
import pandas as pd
import importlib
import time
from django.core import management
from datetime import datetime
from django.db import OperationalError, ProgrammingError


class TableNotConfiguredException(Exception):
    """
    Exception raised for new tables that are not configured in settings.py.
    """
    def __init__(self, msg):
        self.msg = msg + " is not configured in settings.py file"

def get_location():
    return os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

class Migrate_CSV():

    def __init__(self, app_name, batch_size = 100):
        '''
        :param app_name: prefix name of database and the model class in the app 'app_name'
        '''

        if not app_name:
            raise ValueError("Empty " + "app_name")
        self.APP_NAME = app_name.lower().strip()
        self.MODEL_NAME = 'RAW'
        self.DB_NAME = self.APP_NAME + '_db'
        self.__location__ = get_location()
        sys.path.append(os.path.join(self.__location__))
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
        import django
        from django.conf import settings
        print(django.VERSION)
        self.TIMESTAMP_UNIQUE = 'TIMESTAMP'
        self.batch_size = batch_size #for postgres bigger 100 for sqllite 8
        self.INSTALLED_APPS = settings.INSTALLED_APPS
        self.DATABASES = settings.DATABASES
        self.elastic_url = 'http://localhost:9200/'
        self.__check_create_installed_apps()


    def upload_db(self, csv_file, override = False, timestamp_field_name = 'Timestamp', elastic=True):
        '''
        :param csv_file:
        :param timestamp_field_name:
        :param override: 
        '''
        if not os.path.isfile(csv_file):
            raise FileNotFoundError(csv_file)
        if not timestamp_field_name:
            raise ValueError("Empty " + "timestamp_field_name")
        self.csv_file = csv_file
        self.TIMESTAMP_CSV = timestamp_field_name
        is_migrate = self.__prepare_models()
        if is_migrate:
            self.__migrate()
        else:
            print('No migrations needed')

        self.__save_db(override=override)
        if elastic and 'postgr' in self.DATABASES[self.DB_NAME]['ENGINE']:
            print('Create index in Elasticsearch...')
            self.__create_index_zombo(override)

    def __check_create_installed_apps(self):
        for app_name in self.INSTALLED_APPS:
            folder_path = os.path.join(self.__location__, app_name)
            if not os.path.isdir(folder_path):
                os.makedirs(folder_path)
                open(os.path.join(folder_path, '__init__.py'), 'w').close()

    def __create_index_zombo(self, override = False):
        from django.db import connections
        query_extension = "CREATE EXTENSION IF NOT EXISTS zombodb;"
        query_index = """
        CREATE INDEX IF NOT EXISTS idx_zdb_{0}
          ON {0}_{1}
        USING zombodb(zdb('{0}_{1}', {0}_{1}.ctid), zdb({0}_{1}))
        WITH (url='{2}', shards=1, replicas=1);
        """.format(self.APP_NAME, self.MODEL_NAME.lower(), self.elastic_url)

        with connections[self.DB_NAME].cursor() as cursor:
            cursor.execute(query_extension)
            cursor.execute(query_index)
            if override:
                cursor.execute("VACUUM;")

    def __get_existing_columns(self):
        from django.db import connections
        query = "select * from %s_%s limit 0" % (self.APP_NAME, self.MODEL_NAME.lower())
        try:
            with connections[self.DB_NAME].cursor() as cursor:
                cursor.execute(query)
                col_names = [desc[0] for desc in cursor.description]
                return set(col_names) - {'id'}
        except (OperationalError, ProgrammingError) as e:
            print(e)
            return set()


    def __prepare_column_names(self):
        columns = pd.read_csv(self.csv_file, nrows=0).columns
        # make the timestamp name to be the same among all the databases
        column_names = set(columns) - {self.TIMESTAMP_CSV} | {self.TIMESTAMP_UNIQUE}
        column_names = set([name.strip().upper() for name in column_names])
        existing_columns = self.__get_existing_columns()
        is_migrate = bool(column_names - existing_columns)
        print('existing_columns', existing_columns)
        column_names |= existing_columns if existing_columns else set()
        column_names = set(filter(lambda x : x, column_names))
        print('column_names', column_names)
        return column_names, is_migrate

    def __django_resetup(self):
        '''
        django.setup() должна выызваться только один раз что вызвает проблемы динамичного обновления моделей в apps
        этот метод представляет из себя маленький хак для динамического обновления моделей
        :return: 
        '''
        from django.apps import apps
        # with the configuration of loaded apps
        apps.app_configs = OrderedDict()
        # set ready to false so that populate will work
        apps.ready = False
        # re-initialize them all; is there a way to add just one without reloading them all?
        print('APP: ',self.INSTALLED_APPS)
        apps.populate(self.INSTALLED_APPS)


    def __prepare_models(self):
        # check if such table exists in settings
        import django
        django.setup()
        if self.DB_NAME not in self.DATABASES.keys():
            raise TableNotConfiguredException(self.DB_NAME)

        column_names, is_migrate = self.__prepare_column_names()
        # autogenerate model
        if is_migrate or not os.path.exists(os.path.join(self.__location__, self.APP_NAME, 'models.py')):
            source = 'from django.db import models\n\n'
            source += 'class %s(models.Model):\n' % self.MODEL_NAME
            source += '\n'.join(
                ["\t%s = models.FloatField(null=True, db_column='%s')" %
                 (name.replace('_', ''), name) for name in (column_names - {self.TIMESTAMP_UNIQUE})]
            )
            source += '\n\t' + self.TIMESTAMP_UNIQUE + ' = models.DateTimeField(null=False, unique=True)'
            source += "\n\tdef __str__(self):\n\t\treturn 'RAW: ' + str(self.TIMESTAMP)"
            with open(os.path.join(self.__location__, self.APP_NAME, 'models.py'), 'w') as fw:
                fw.write(source)
                fw.flush()
                os.fsync(fw.fileno())

        return is_migrate

    def __migrate(self):
        '''
        python manage.py migrate %s --database='%s_db'
        python manage.py migrate %s
        '''
        self.__django_resetup()
        print('Migration start... ' + self.APP_NAME)

        # создать SQL скрипты
        management.call_command('makemigrations', self.APP_NAME, interactive=False)
        # провести миграцию
        management.call_command('migrate', self.APP_NAME, '--database='+self.DB_NAME, interactive=False)

    # def __remove

    def __save_db(self, override=False):
        import django
        django.setup()

        models = importlib.import_module('%s.models' % self.APP_NAME)
        s = time.time()

        entries = self.__prepare_entries_save(override)
        print('len', len(entries))

        entries = [models.RAW(**x) for x in entries]
        if entries:
            print(entries[0])
            print(entries[-1])

        models.RAW.objects.bulk_create(entries, self.batch_size)
        print('Time elapsed on db save %s s.' % str(time.time() - s))

    def __prepare_entries_save(self, override):
        s = time.time()
        df = pd.read_csv(self.csv_file, parse_dates=[self.TIMESTAMP_CSV]) #nrows=1000
        df[self.TIMESTAMP_CSV] = df[self.TIMESTAMP_CSV].astype(datetime)
        columns = list(df.columns)
        columns[columns.index(self.TIMESTAMP_CSV)] = self.TIMESTAMP_UNIQUE
        columns = [x.replace('_', '').upper() for x in columns]
        df.columns = columns
        models = importlib.import_module('%s.models' % self.APP_NAME)
        print('df size before filter: ', df.shape[0])
        if override:
            count_deleted = 0
            size = 100
            existing_list  = list(df[self.TIMESTAMP_UNIQUE].values)
            for pos in range(0, len(existing_list), size):
                existing_delete= existing_list[pos:pos + size]
                ids_delete = models.RAW.objects.filter(**{self.TIMESTAMP_UNIQUE + '__in': existing_delete}).values_list('pk', flat=True)
                count_deleted += models.RAW.objects.filter(pk__in=ids_delete).delete()[0]
            # models.RAW.objects.filter(**{self.TIMESTAMP_UNIQUE + '__in': existing}).delete()[0]
            print('deleted from db: ',count_deleted )
        else:
            existing = set(models.RAW.objects.values_list(self.TIMESTAMP_UNIQUE, flat=True))

            df = df[~df[self.TIMESTAMP_UNIQUE].isin(existing)]
        print('df size after: ', df.shape[0])
        entries = df.to_dict('records')
        print('Time elapsed on csv file data reading %s s.' % str(time.time() - s))
        return entries

def upload(csv_file, app_name, override=False, timestamp_field_name="Timestamp", batch_size = 100, elastic=True):
    migrator = Migrate_CSV(app_name, batch_size=batch_size)
    migrator.upload_db(csv_file, override=override, timestamp_field_name=timestamp_field_name, elastic=elastic)

if __name__ == '__main__':
    csv_file = '.data_test/a.csv'
    upload(csv_file, 'testload', override=True, timestamp_field_name="Timestamp", batch_size=400, elastic=True)