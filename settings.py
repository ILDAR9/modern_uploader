import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '%07fd(a@64v*4le1#k@6!has=jz8mpdq$qqsr7(mounn#^zs3x'

# Application definition
INSTALLED_APPS = [
    'trade',
    'testload'
]

# If you set this to False, Django will not use timezone-aware datetimes.
TIME_ZONE = 'UTC'


DATABASE_ROUTERS = ['DBRouter.DBRouter']

# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases
DATABASES = {
    'default': {},
    # 'sibur_db': {
    #     'ENGINE': 'django.db.backends.sqlite3',
    #     'NAME': os.path.join(BASE_DIR, 'sibur_db.sqlite3'),
    # },
    'trade_db': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'sibur',
        'USER': 'user',
        'PASSWORD': 'password',
        'HOST': 'localhost',
        'PORT': '5432',
    },
    'testload_db': {
        # 'ENGINE': 'django.db.backends.sqlite3',
        # 'NAME': os.path.join(BASE_DIR, 'test_db.sqlite3'),
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'testload',
        'USER': 'user',
        'PASSWORD': 'password',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}