# Сбор данных о запросах из API Yandex Мониторинг поисковых запросов
Скрипт-сборщик данных из API Мониторинг поисковых запросов Яндекс.
https://webmaster.yandex.ru/blog/vstrechayte-api-monitoringa-poiskovykh-zaprosov

Скрипт решает проблему ограниченности данных четырнадцатью днями. В ЯВМ данные указываются только за последние две недели. Скрипт же собирает данные последовательно, накопляя их.

## Описание данных в БД.
Собираются все запросы по каждому url за каждый день, по которым был спрос и показы.
#### Столбцы
    URL - url
    QUERY - запрос
    DATE - дата показа
    DEMAND - спрос
    IMPRESSIONS - показ
    CLICKS - клик
    CTR - процент кликабельности
    POSITION - позиция
#### Особенности
Спрос собирается в том случае, если пользователем поисковой системы был введен поисковый запрос в поиск.
Показы же собираются только в том случае, если пользователь увидил ссылку на сайт. Иначе говоря, если ссылка была на второй странице выдачи, а пользователь остался на первой, то показ не будет засчитан, в свою очередь - спрос учтется.
Это значит, что зависимый от показателя *показы* показатель *позиции* будет не известен. Поэтому в БД для такой ячейки буедт NULL. Как интерпретировать *позицию* с NULL - как 51 по умолчанию (для подсчета средней позиции) - или игнорировать при рассчетах средней - решение остается за вами. Подчеркну, что запрос может быть, к примеру, на 12 месте, но пользователь не перейдет на вторую страницу SERP'а, и таким образом засчитается NULL.

Еще одна особенность: запросы с показателем *спрос* (DEMAND) равным нулю не собираются в БД несмотря на то, что в статистике ЯВМ и API они отражены. Это необходимо для экономии места. На арифметические операции с данными это повлиять не должно, т.к. "не было ввода запроса - не было и статистики".

## Примечание
Скрипт и все настройки тестировались только на сборке Ubuntu 22.04 Jammy.

## Технологии
- Python 3.10
- MariaDB 10.6.16

## Получение токена API Яндекс Мониторинг позиций
https://yandex.ru/dev/webmaster/doc/dg/tasks/how-to-get-oauth.html

## Получение идентификатора пользователя
https://yandex.ru/dev/webmaster/doc/dg/reference/user.html

## Последовательность действий
#### Установка БД
1. Производим обновления.
```
apt update && sudo apt upgrade
```
2. Проверяем наличие mariaDB.
```
systemctl status mariadb
```
3. Устанавливаем mariaDB, если нет.
```
apt install mariadb-server
```
4. *Если DB будет размещена на удаленном сервере, открываем доступ, иначе - ничего не меняем.
```
nano /etc/mysql/mariadb.conf.d/50-server.cnf
    #Заменяем bind-address  = 127.0.0.1
	bind-address            = 0.0.0.0
```
5. Запускаем безопасный установщик mariaDB для установки базовых настроек безопасности, пароля для root по умолчанию нет, нужно будет создать.
```
mysql_secure_installation
```
6. Перезапускаем.
```
systemctl restart mariadb
```
7. Проверяем.
```
systemctl status mariadb
```
#### Настойка БД
1. Заходим в настройки базы.
```
mysql -u root -p 
```
2. Создаем новую БД.
```
CREATE DATABASE main;
```
3. Создаем нового пользователя и придумываем пароль.
```
CREATE USER 'main_user'@'localhost' IDENTIFIED BY '*****';
```
4. Даем пользователю привелегии для работы с созданной БД.
```
GRANT ALL PRIVILEGES ON main.* TO 'main_user'@'localhost';
```
5. Принимаем привелегии.
```
FLUSH PRIVILEGES;
```
#### Установка python
1. Проверяем наличие python3.10, если нет, то устанавливаем.
```
python --version
```
2. Устанавливаем python3.10
```
apt install python3.10 -y
```
3. Устанавливаем venv
```
apt install python3.10-venv
```
4. Устанавливаем git
```
apt-get install git
```
5. Проверяем наличие pip, его иногда может не быть.
```
pip --version
```
6. Если pip нет, то переходим в папку с питоном.
```
cd /usr/lib/python3.10
```
7. Скачиваем установщик pip.
```
wget https://bootstrap.pypa.io/get-pip.py
```
8. Устанавливаем pip.
```
python3.10 get-pip.py
```
9. Проверяем.
```
pip --version
```
#### Установка скрипта
1. Создаем папку, где поместим скрипт.
```
mkdir /usr/local/py_scripts && cd /usr/local/py_scripts
```
2. Загружаем скрипт.
```
git clone https://github.com/Edbaro42/ywm_mon_pos_api.git
```
3. Переходим в директорию со скриптом.
```
cd /usr/local/py_scripts/ywm_mon_pos_api
```
4. Включаем venv.
```
python3.10 -m venv venv
```
5. Запускаем venv.
```
source venv/bin/activate
```
6. Обновляем pip.
```
python3.10 -m pip install --upgrade pip
```
7. Загружаем все зависимости для скрипта.
```
pip install -r requirements.txt
```
#### Настраиваем скрипт
1. Создаем файл .env и указываем все необходимые для работы данные.
```
nano .env
```
    API_URL = https://api.webmaster.yandex.net/v4/user/{user-id}/hosts/{host-id}/query-analytics/list #Адрес запроса к api.
    HEADERS = OAuth y0_example_token #Токен API.
    HOST = localhost #Если БД на этом же сервере, то оставляем без изменений, иначе указываем IP.
    USER = example_user #Указываем имя созданного ранее юзера в БД.
    PASSWORD = example_password #Указываем пароль для созданного ранее юзера в БД.
    DB = your_db_name #Указываем имя ранее созданной БД.

2. проверяем время сервера, это нужно, чтобы правильно выставить задачу в cron.
```
date
```
3. Запускаем редактирование cron, выбираем nano.
```
crontab -e
```
4. Создаем ежедневную задачу с логированием.
```
18 13 * * * /usr/local/py_scripts/ywm_mon_pos_api/venv/bin/python3.10 /usr/local/py_scripts/ywm_mon_pos_api/main.py >> /usr/local/py_scripts/ywm_mon_pos_api/logfile.log 2>&1
```
## Волшебные числа
1. Про эти можно почитать в документации к API [тут](https://yandex.ru/dev/webmaster/doc/dg/reference/host-query-analytics.html#request-format__device-type-ind) и [тут](https://yandex.ru/dev/webmaster/doc/dg/reference/host-query-analytics.html#request-format__region-ids)
```
GEO = 11079 # 225, 1, 10174, 11079 
DEVICE = 'ALL' # ALL, DESKTOP, MOBILE_AND_TABLET, MOBILE, TABLET
```
2. Эти цифры
```
SLEEP_TIME_API = 0.5
MAX_ATTEMPTS = 5
SLEEP_TIME_ERR = 60
```
  - Таймаут в сек. обращений к API (есть ограничение - не более 10к запросов в час).
  - Количество повторений в случае неудачного обращения к API или БД.
  - Таймаут в сек. на случай, если было неудачное обращение к API или БД.

## Автор
- [Ed](https://github.com/Edbaro42/)
- [Telegram](https://t.me/Edbaro42)