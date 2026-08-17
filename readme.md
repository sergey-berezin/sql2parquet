# sql2parquet - simple fast command line utility to export SQL to parquent files

## 1. Сборка


```bash
go mod tidy
go build -o sql2parquet .
```

---

## 2. Примеры запуска

### PostgreSQL через отдельные параметры

```bash
./sql2parquet \
  -d postgres \
  -H localhost \
  -P 5432 \
  -D sales \
  -u postgres \
  -p secret \
  -S "select id, created_at, amount from orders" \
  -o orders.parquet
```

### PostgreSQL через строку соединения

```bash
./sql2parquet \
  -d postgres \
  -c "postgres://postgres:secret@localhost:5432/sales?sslmode=disable" \
  -S "select * from orders" \
  -o orders.parquet
```

### Oracle через service name

```bash
./sql2parquet \
  -d oracle \
  -H db.example.com \
  -P 1521 \
  -D ORCLPDB1 \
  -u scott \
  -p tiger \
  -S "select * from sales" \
  -o sales.parquet
```

### Oracle через SID

```bash
./sql2parquet \
  -d oracle \
  -H db.example.com \
  -P 1521 \
  -D SID:XE \
  -u scott \
  -p tiger \
  -S "select * from sales" \
  -o sales.parquet
```

---

## 3. Как выполняются нефункциональные требования

### Большие запросы

Утилита не делает `SELECT` в память целиком:
- `database/sql.Rows` читается потоково;
- строки обрабатываются пачками через bounded-каналы;
- размер пачки задаётся переменной окружения `SQL2PARQUET_BATCH_SIZE`, по умолчанию `2048`;
- если нужно уменьшить потребление памяти, уменьшите её, например:

```bash
SQL2PARQUET_BATCH_SIZE=256 ./sql2parquet ...
```

### Параллельность

В реализации есть несколько уровней параллельности:
1. Отдельная горутина читает строки из БД.
2. Пул горутин параллельно конвертирует строки в значения для Parquet.
3. Библиотека `parquet-go` получает `np = workers` и может параллельно кодировать страницы/данные.
4. Запись в файл идёт упорядоченно, чтобы сохранять порядок строк результата.

Число потоков можно задать так:

```bash
SQL2PARQUET_WORKERS=8 ./sql2parquet ...
```

---

## 4. Особенности и ограничения

1. **Имена колонок**  
   Имена колонок из SQL приводятся к безопасному виду: спецсимволы заменяются на `_`, дубликаты устраняются суффиксами `_2`, `_3` и т.п.

2. **Типы данных**  
   Используется безопасное динамическое сопоставление:
   - целые SQL-типы → `INT64`;
   - float/real/double → `DOUBLE`;
   - boolean → `BOOLEAN`;
   - бинарные данные → `BYTE_ARRAY`;
   - даты/времена/JSON/UUID/CLOB/прочее → `BYTE_ARRAY UTF8`.

   Если нужна более точная Parquet-схема, например `TIMESTAMP_MICROS`, `DATE`, `DECIMAL(precision,scale)`, можно расширить функции `chooseTarget`, `schemaEntry` и конвертацию значений.

3. **NUMERIC/NUMBER с высокой точностью**  
   Большие `NUMERIC`/`NUMBER` сохраняются как строки, чтобы не терять точность. Если нужен числовой Parquet-тип `DECIMAL`, это нужно добавлять отдельно.

4. **Параллельное чтение из БД**  
   В общем случае один SQL-запрос нельзя безопасно разрезать на параллельные части без partition-колонки. Если нужна именно параллельная вычитка несколькими соединениями, нужно отдельно реализовывать разбиение, например:

   ```sql
   where id between ? and ?
   ```

   или `row_number()`-партиционирование для Oracle/PostgreSQL.

5. **Пароль в командной строке**  
   Пароль, переданный через `-p`, может быть виден в списке процессов. Для промышленного использования лучше применять строку соединения, переменные окружения или secrets-менеджер.
