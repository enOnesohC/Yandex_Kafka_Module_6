# Инструкция по проверке

## Описание проекта

Данный проект показывает работу с облачными кластерами, использование схем для валидации сообщений,
а также использование NIFI для разработки простого пайплайна Топик -> БД 

Используемые контейнеры:

- kafka: кластер kafka из трёх брокеров 
- ui kafka: интерфейс для взаимодействия с брокерами kafka
- schema-registry: контейнер со схемами сообщений
- nifi: для разработки пайплайнов, обработки данных
- postgres: бд PostgreSQL

- Main.py - файл для проверки задания
- ProducerObject.py - файл с классом продьюсера
- ConsumerObject.py - файл с классом консьюмера  

## Шаг 1.
Проверьте, установлен ли Docker на вашем компьютере. Если нет, скачайте и установите его с официального сайта Docker.
Проверьте, что Docker работает, выполнив команду в терминале:

docker --version 
Ожидаемый вывод: версия установленного Docker.

## Шаг 2.

В терминале перейдите в папку, в которой будет располагаться тестовый проект
Выполнить команду:
git clone https://github.com/enOnesohC/Yandex_Kafka_Module_6.git

Ожидаемый результат: устанавливается тестовое окружение в папку

## Шаг 3.

Откройте терминал в папке с файлом docker-compose.yaml

Для запуска всех сервисов используйте команду:
docker-compose up 

## Шаг 4.

Необходимо выполнить настройку проекта:

- скачать файл postgresql-42.7.14.jar, поместить его в папку infra;

-Зайти по адресу http://localhost:8090/nifi/
-Создать объект Process Group
-При создании объекта указать файл Nifi_Flow.json в корне проекта.

Будет развёрнуто тестовое решение.

Далее, необходимо с помощью двойного клика развернуть созданный объект.
После этого, необходимо открыть PutDatabaseRecord, перейти в Properties->DBCPConnectionPool
Необходимо задать пароль пользователя, он не сохраняется при передаче.
Значение указано в docker-compose: postgres-pw

После этого проект готов к проверке.

## Шаг 5.

Предусловие:
развёрнуты контейнеры, flow Nifi настроен по инструкции.

Сценарий:

- выполняем команду для захода в БД: docker exec -it postgres psql -U postgres-user -d users 
- проверяем, что в БД отсутствуют записи, для этого выполняем запрос: select * from users; 
- Далее, необходимо запустить файл Main.py


Продьюсер отправит 6 сообщений, будет вывод в консоль:
Delivered message to topic user [0] at offset 43
Delivered message to topic user [0] at offset 44
Delivered message to topic user [1] at offset 40
Delivered message to topic user [1] at offset 41
Delivered message to topic user [1] at offset 42
Delivered message to topic user [2] at offset 48


Далее, консьюмер прочитает сообщения, будет вывод в консоль:
{'id': 1, 'name': 'First user', 'favoriteNumber': 42, 'favoriteColor': 'blue'}
Headers: [('myTestHeader', b'header values are binary')]
Message on user:
{'id': 2, 'name': 'Second user', 'favoriteNumber': 2, 'favoriteColor': 'red'}
Headers: [('myTestHeader', b'header values are binary')]
Message on user:
{'id': 3, 'name': 'Third user', 'favoriteNumber': 3, 'favoriteColor': 'blue'}
Headers: [('myTestHeader', b'header values are binary')]
Message on user:
{'id': 4, 'name': 'Four user', 'favoriteNumber': 4, 'favoriteColor': 'green'}
Headers: [('myTestHeader', b'header values are binary')]
Message on user:
{'id': 5, 'name': 'Five user', 'favoriteNumber': 5, 'favoriteColor': 'grey'}
Headers: [('myTestHeader', b'header values are binary')]
Message on user:
{'id': 6, 'name': 'Six user', 'favoriteNumber': 6, 'favoriteColor': 'grey'}
Headers: [('myTestHeader', b'header values are binary')]

После этого, опять заходим в бд и проверяем наличие записей в БД. Должно быть 6 записей:
users=# select * from users;
 id |    name     | favoritenumber | favoritecolor
----+-------------+----------------+---------------
  1 | First user  | 42             | blue
  2 | Second user | 2              | red
  6 | Six user    | 6              | grey
  3 | Third user  | 3              | blue
  4 | Four user   | 4              | green
  5 | Five user   | 5              | grey
(6 rows)


В рамках задания не получилось использовать кластер в облаке, но есть классы Продьюсера и Консьюмера 
для взаимодействия с облачным кластером(в задании используются обычные классы)

в файле cluster-config лежат настройки предполагаемого кластера kafka.
в файле commands лежат команды, которые необходимо ввести для проверки
в папке screens лежат скриншоты, требуемые по заданию.
в файле logs лежат логи контейнера Nifi при отправке сообщения.
