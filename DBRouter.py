class DBRouter:
    """
    A router to control all database operations on models
    """
    def db_for_read(self, model, **hints):
        """
        Attempts to read app models go to app_db.
        """
        if model._meta.app_label == 'trade':
            return 'trade_db'
        elif model._meta.app_label == 'testload':
            return 'testload_db'
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write the app models go to the app_db.
        """
        if model._meta.app_label == 'trade':
            return 'trade_db'
        elif model._meta.app_label == 'testload':
            return 'testload_db'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model in the app is involved.
        """
        if obj1._meta.app_label == 'trade' or \
           obj2._meta.app_label == 'trade':
           return True
        elif obj1._meta.app_label == 'testload' or \
           obj2._meta.app_label == 'testload':
           return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the app only appears in the 'app_db'
        database.
        """
        if app_label == 'trade':
            return db == 'trade_db'
        elif app_label == 'testload':
            return 'testload_db'
        return None