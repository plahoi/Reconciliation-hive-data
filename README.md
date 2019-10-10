Система проверки данных (реконсиляции) двух таблиц (файлов) в Hadoop Hive.

## Критерии проверок для edq
- Проверка должна быть простой.
- В проверке должна отсутствовать сложная аналитическая логика.
- Результатом проверки является список некорректных строк в проверяемых таблицах (файлах).
- Следствием результата проведения edq должно быть решение о необходимости исправления данных.
- Проверка происходит очень быстро


# Структура джоба __recon__<br>
Проект является `spark-submit` джобом

## Build package.zip
Для работы проекта необходимо собрать все используемые зависимости в один файл, передаваемый на ноды с помощью `--py-files` аргумента
```shell
bash calcDQ/build.sh
```
 
## Run job
```shell
spark-submit --master yarn \
--py-files recon/package.zip \
recon/run.py -src sandbox_apolyakov.recon_source -dst sandbox_apolyakov.recon_dest -j recon/table_name_recon_config.json
```


## Проверки пакета recon
|Validaion|Description|
|-|-|
|policies.validation.int_validator|`args: tolerance`: Выполняет сверку значений типа `int` с опционально заданной погрешностью|
|policies.validation.string_validator|Выполняет сверку значений типа `string`|
|policies.validation.date_validator|Выполняет сверку значений типа `date`|


## Файловая структура проекта recon
```
recon
│   build.sh
│   README.md
│   run.py
│   table_name_recon_config.json
│   __init__.py
│
├───policies
│       validation.py
│       __init__.py
│
└───src
        recon.py
        __init__.py
```

Входной точкой является `run.py`
|||
|-|-|
|`build.sh`|Необходимо запускать каждый раз при обновлении папок policies, src|
|`table_name_recon_config.json`|Файл-конфигурация адаптера для реконсиляции двух наборов данных|
|`policies.validation`|Файл с валидациями|
|`src.recon`|Основной код для работы с данными и валидациями|

Пример конфигурационного файла `table_name_recon_config.json`<br>
Имя файла может состоять из названия проверяемой сущности либо процесса

```json
{
    "key": "id",
    "col_1": {
        "dst_column_name": "name",
        "src_column_name": "name",
        "validation": {
            "properties": {},
            "validationMethod": "string_validator"
        }
    },
    "col_2": {
        "dst_column_name": "age",
        "src_column_name": "vozrast",
        "validation": {
            "properties": {
                "tolerance": 0.1
            },
            "validationMethod": "int_validator"
        }
    },
    "col_3": {
        "dst_column_name": "day",
        "src_column_name": "day",
        "validation": {
            "properties": {},
            "validationMethod": "date_validator"
        }
    }
}
```

- `key` - Название колонки с уникальным ключом. В данном примере подразумевается, что ключ имеет одинаковое название в обоих наборах данных. В случае разного названия - необходимо доработать джоб
- `col_*` - Плейсхолдер для хранения словаря каждой проверяемой колонки
    - `dst_column_name` - Название колонки в таблице(файле) назначения
    - `src_column_name` - Название колонки в источнике
    - `validation` - Валидация колонки (в данном примере одна на колонку но может быть несколько при доработке кода)
        - `properties` - Передаваемые в валидацию параметры
        - `validationMethod` - Используемый для валидации метод


## Как это работает
В данном примере:
1. данные читаются из таблиц, передаваемых джобу в параметрах `src` и `dst`
1. Политики validation применяются на каждую заданную колонку для каждой переданной строки
1. Данные с ошибками складываются в датафрейм, в дальнейшем доступный для записи в любую бд или файл.


> Валидация никак не воздействует на существующий процесс работы интеграций и загрузки данных. Она происходит параллельно с существующей загрузкой данных в целевые таблицы и служит исключительно в качестве средства наблюдения.

Инициализирующий код для `Hive`
```sql
drop table if exists sandbox_apolyakov.recon_source;
create table sandbox_apolyakov.recon_source (
    id int,
	name string,
  	vozrast int,
  	day date
);
insert into sandbox_apolyakov.recon_source values
(1, 'Dow', 33, '2018-07-13'),
(2, 'Pits', 122, '1917-04-12'), 
(3, 'Chris', 38, '2001-05-26'), 
(4, 'James', 21, '2001-05-26'), 
(5, 'Penelopa', 38, '2001-05-30');

drop table if exists sandbox_apolyakov.recon_dest;
create table sandbox_apolyakov.recon_dest (
    id int,
	name string,
  	age int,
  	day date
);
insert into sandbox_apolyakov.recon_dest values
(1, 'Dow', 33, '2018-07-13'), -- valid
(2, 'Pits', 120, '1917-04-10'), -- invalid age (122 -> 120) and date (1917-04-12 -> 1917-04-10)
(3, 'Charles', 38, '2001-05-26'), -- invalid name (Chris -> Charles)
(4, 'James', 200, '2001-05-26'), -- invalid age (21 -> 200)
(55, 'Penelopa', 38, '2001-05-10') -- invalid date (2001-05-30 -> 2001-05-10) and Index
;
```

>Тестирование проводилось на Hadoop Hortonworks 2.6.5, Spark 2.3.1, Python 2.7