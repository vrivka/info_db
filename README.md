INFO 21
========
Реализация web-интерфейса для postgreSQL базы данных.  
Проект был выполнен вместе с [trubyroid](https://github.com/trubyroid/) в рамках обучения в Школе 21. 

Основной функционал:  
- CRUD-операции
- Возможность ввода пользовательского sql-запроса
- Отображение и возможность использовать хранимые процедуры/функции
- Импорт таблиц из .csv
- Экспорт таблиц/результатов запроса/результатов функций в .csv

## Старт
>[!IMPORTANT]
> Для работы с проектом необходимо заполнить базу данных (например, файлами из директории database).  
> Также нужно добавить файл **.env** в директорию **project** с указанием переменных для установки соединения с бд:  
> DATABASE, POSTG_USER, POSTG_PASW, POSTG_HOST и POSTG_PORT

1. Для установки пакетов - `pip install -r requirements.txt`
3. Для запуска приложения на **http://127.0.0.1:5000** - `flask run` в директории **project**

## Описание с примерами

### Menu
![Menu](./images/menu_screen.png)  
- `Home` и нажатие по логотипу отправляет на главную страницу с информацией о проекте и разработчиках.
- `Data` предоставит выбор таблицы для чтения, редактирования, импорта и экспорта
- `Operations` хранит в себе разделы `Custom query` и `Stored functions`

### Data 
![Data](./images/data.gif)  
### Custom query
![Custom query](./images/custom_query.gif)  
### Stored functions
![Functions](./images/stored_functions.gif)  

>[!NOTE]
> Во всех полях ввода стоит защита от SQL-инъекций.

## Завершение
Благодарим читателя за интерес к проекту, будем рады вашей обратной связи!