# Django tutorial
- [Django tutorial](#django-tutorial)
  - [Create Project](#create-project)
    - [Init Project](#init-project)
    - [Python Virtualenv](#python-virtualenv)
    - [configuration](#configuration)
    - [Testing](#testing)
  - [Create Polls APP](#create-polls-app)
    - [Init](#init)
    - [Implement View & Add route](#implement-view--add-route)
    - [Test](#test)
  - [Database](#database)
    - [DB Init](#db-init)
    - [Use pymysql with Django](#use-pymysql-with-django)
    - [Test Database](#test-database)
  - [Add models](#add-models)
    - [Add models](#add-models-1)
    - [Migration](#migration)
  - [Shell](#shell)
  - [Admin User](#admin-user)

## Create Project

### Init Project

```
django-admin startproject mysite
```

```
mysite
├── manage.py
└── mysite
    ├── asgi.py
    ├── __init__.py
    ├── settings.py
    ├── urls.py
    └── wsgi.py
```
These files are:

The outer mysite/ root directory is a container for your project. Its name doesn’t matter to Django; you can rename it to anything you like.
- manage.py: A command-line utility that lets you interact with this Django project in various ways. You can read all the details about manage.py in django-admin and manage.py.
- The inner mysite/ directory is the actual Python package for your project. Its name is the Python package name you’ll need to use to import anything inside it (e.g. mysite.urls).
- mysite/__init__.py: An empty file that tells Python that this directory should be considered a Python package. If you’re a Python beginner, read more about packages in the official Python docs.
- mysite/settings.py: Settings/configuration for this Django project. Django settings will tell you all about how settings work.
- mysite/urls.py: The URL declarations for this Django project; a “table of contents” of your Django-powered site. You can read more about URLs in URL dispatcher.
- mysite/asgi.py: An entry-point for ASGI-compatible web servers to serve your project. See How to deploy with ASGI for more details.
- mysite/wsgi.py: An entry-point for WSGI-compatible web servers to serve your project. See How to deploy with WSGI for more details.

### Python Virtualenv

```
virtualenv venv
source venv/bin/activate
vim requirements.txt
pip install -r requirements.txt
```

### configuration

```
vim mysite/settings.py
```

```diff
$ git diff mysite/settings.py 
diff --git a/mysite/settings.py b/mysite/settings.py
index 2f737ba..92e82b8 100644
--- a/mysite/settings.py
+++ b/mysite/settings.py
@@ -25,7 +25,7 @@ SECRET_KEY = 'django-insecure-cl*-fu&y+-=65ykkqe_w+1s*r3ye=jz#fdg&8jd!v1pkf+pt1(
 # SECURITY WARNING: don't run with debug turned on in production!
 DEBUG = True
 
-ALLOWED_HOSTS = []
+ALLOWED_HOSTS = ['*']
 
 
 # Application definition
@@ -75,8 +75,8 @@ WSGI_APPLICATION = 'mysite.wsgi.application'
 
 DATABASES = {
     'default': {
-        'ENGINE': 'django.db.backends.sqlite3',
-        'NAME': BASE_DIR / 'db.sqlite3',
+        #    'ENGINE': 'django.db.backends.sqlite3',
+        #    'NAME': BASE_DIR / 'db.sqlite3',
     }
 }
 ```

 ### Testing

```
python manage.py runserver 0:8000
```

## Create Polls APP

### Init

```
# python manage.py startapp polls
# git diff
# 
```
no changes to original mysite

```
polls/
├── admin.py
├── apps.py
├── __init__.py
├── migrations
│   └── __init__.py
├── models.py
├── tests.py
└── views.py
```

### Implement View & Add route

1. add view logic in polls/views.py
1. add route in polls polls/urls.py, which need created manually
1. add route to pools in mysite/urls.py

### Test

```
# curl http://localhost:8000/polls/
Hello, world. You're at the polls index.
```


## Database

### DB Init

omitted

### Use pymysql with Django

put the follwing in mysite/settings.pymysql

```python
import pymysql

#pymysql.version_info = (1, 4, 2, "final", 0)
pymysql.install_as_MySQLdb()

```

### Test Database

```
python manage.py migrate
```

## Add models

### Add models


1. Change your models (in models.py).
1. Run python manage.py makemigrations to create migrations for those changes
1. Run python manage.py migrate to apply those changes to the database.

### Migration

```
# python manage.py makemigrations polls
Migrations for 'polls':
  polls/migrations/0001_initial.py
    - Create model Question
    - Create model Choice
```

This create file polls/migrations/0001_initial.py

```sql
$ python manage.py sqlmigrate polls 0001
--
-- Create model Question
--
CREATE TABLE `polls_question` (`id` bigint AUTO_INCREMENT NOT NULL PRIMARY KEY, `question_text` varchar(200) NOT NULL, `pub_data` datetime(6) NOT NULL);
--
-- Create model Choice
--
CREATE TABLE `polls_choice` (`id` bigint AUTO_INCREMENT NOT NULL PRIMARY KEY, `choice_text` varchar(200) NOT NULL, `votes` integer NOT NULL, `question_id` bigint NOT NULL);
ALTER TABLE `polls_choice` ADD CONSTRAINT `polls_choice_question_id_c5b4b260_fk_polls_question_id` FOREIGN KEY (`question_id`) REFERENCES `polls_question` (`id`);
```

This show the incoming changes to the DB.

```
# python manage.py migrate
Operations to perform:
  Apply all migrations: admin, auth, contenttypes, polls, sessions
Running migrations:
  Applying polls.0001_initial... OK
```


## Shell

```python
>>> from polls.models import Choice, Question  # Import the model classes we just wrote.

# No questions are in the system yet.
>>> Question.objects.all()
<QuerySet []>

# Create a new Question.
# Support for time zones is enabled in the default settings file, so
# Django expects a datetime with tzinfo for pub_date. Use timezone.now()
# instead of datetime.datetime.now() and it will do the right thing.
>>> from django.utils import timezone
>>> q = Question(question_text="What's new?", pub_date=timezone.now())

# Save the object into the database. You have to call save() explicitly.
>>> q.save()

# Now it has an ID.
>>> q.id
1

# Access model field values via Python attributes.
>>> q.question_text
"What's new?"
>>> q.pub_date
datetime.datetime(2012, 2, 26, 13, 0, 0, 775217, tzinfo=<UTC>)

# Change values by changing the attributes, then calling save().
>>> q.question_text = "What's up?"
>>> q.save()

# objects.all() displays all the questions in the database.
>>> Question.objects.all()
<QuerySet [<Question: Question object (1)>]>
```

## Admin User

```
python manage.py createsuperuser
```

Then go to http://localhost:8000/admin/

Make Polls admin

```diff
 git diff polls/admin.py 
diff --git a/polls/admin.py b/polls/admin.py
index 8c38f3f..d0e6a57 100644
--- a/polls/admin.py
+++ b/polls/admin.py
@@ -1,3 +1,7 @@
 from django.contrib import admin
 
 # Register your models here.
+
+from .models import Question
+
+admin.site.register(Question)
```