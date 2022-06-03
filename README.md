#### Для пользователя:
 - Скачиваем в папку sqlondbf.exe, query.sql, dbf.schema
 - Кладем в туже папку два файла с таблицами, которые нужно "склеить"
 - запускаем sqlondbf.exe, указываем путь к таблице 1 и таблице 2. (тут по умолчанию под таблицей 2 позразумевается та, в котой есть поле DIAG и полис указан в поле NMBPOL)
 - жмем выполнить
 - смотрим результат

Объединение таблиц происходит по условию равенства поля POLIS из первой таблицы и NMBPOL из второй (а не по ФИО!)

#### Для разработчика:
Для работы нужен python 3.7 (другие не проверялись). Скачать можно например тут https://www.python.org/downloads/

Уствновка зависимостей для работы и сборки exe:
 `python.exe -m pip install -r requirements.txt`
 
Если хочется собрать exe, то простой способ это запустить `autopytoexe.exe` (есть в requirements.txt), выбрать там sqlondb.py, one file, window based и нажать convert.  

Принцип работы:
  - читаем исходные таблицы
  - пишем их в обе в sqlite пытаясь сохранить типы колонок
  - выполняем над ними sql запрос из файла запроса (query.sql по умолчанию)
  - сохраняем результат запроса в указаном формате
 
Без проблем с типами работает адекватно только если обе исходные таблицы в dbf и результат тоже пишется в dbf. Все в кодировке cp-866 (ака "дос кодировка")
При этом для случая записи dbf есть костыль, который ждет, что в папке с программой есть файл dbf.schema и там указана схема таблицы, которая должна получиться.
query.sql и dbf.schema подходят для того единственного варианта, который на практике был нужен заказчику.


Если есть вопросы можно попробовать завести issue прямо тут на гитхабе.