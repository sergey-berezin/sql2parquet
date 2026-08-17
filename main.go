package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"sync/atomic"
	_ "github.com/jackc/pgx/v5/stdlib"
	local "github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	_ "github.com/sijms/go-ora/v2"
)

type targetKind int

const (
	targetString targetKind = iota
	targetInt64
	targetDouble
	targetBool
	targetBytes
)

type columnDef struct {
	Name   string
	Target targetKind
}

type options struct {
	driver    string
	connStr   string
	host      string
	database  string
	user      string
	password  string
	port      int
	query     string
	output    string
	workers   int
	batchSize int
}

func main() {
	opts := parseFlags()

	if err := validateOptions(&opts); err != nil {
		log.Fatalf("Ошибка аргументов: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := export(ctx, opts); err != nil {
		log.Fatalf("Ошибка экспорта: %v", err)
	}
}

func parseFlags() options {
	var opts options
	var connStrCyr string

	flag.StringVar(&opts.database, "D", "", "имя базы данных: dbname для PostgreSQL или service name для Oracle; для Oracle можно задать SID в виде SID:XE")
	flag.StringVar(&opts.driver, "d", "", "драйвер: oracle | postgres")
	flag.StringVar(&opts.connStr, "c", "", "строка соединения, перекрывает -H/-P/-D/-u/-p")
	flag.StringVar(&connStrCyr, "с", "", "строка соединения, кириллическая буква 'с'")
	flag.StringVar(&opts.user, "u", "", "имя пользователя")
	flag.StringVar(&opts.password, "p", "", "пароль пользователя")
	flag.IntVar(&opts.port, "P", 0, "порт")
	flag.StringVar(&opts.host, "H", "", "хост")
	flag.StringVar(&opts.query, "S", "", "команда SQL")
	flag.StringVar(&opts.output, "o", "", "выходной файл parquet")

	flag.Usage = func() {
		out := flag.CommandLine.Output()
		fmt.Fprintf(out, "Утилита экспорта результата SQL-запроса в Parquet.\n\n")
		fmt.Fprintf(out, "Использование: sql2parquet -d oracle|postgres -S 'SELECT ...' -o out.parquet [опции]\n\n")
		flag.PrintDefaults()
		fmt.Fprintf(out, "\nПеременные окружения для настройки памяти/параллелизма:\n")
		fmt.Fprintf(out, "  SQL2PARQUET_WORKERS     - число потоков конвертации и кодирования Parquet (по умолчанию NumCPU)\n")
		fmt.Fprintf(out, "  SQL2PARQUET_BATCH_SIZE  - размер пачки строк в памяти (по умолчанию 2048)\n")
	}

	flag.Parse()

	// Поддержка и латинской -c, и случайно набранной кириллической -с.
	if opts.connStr == "" && connStrCyr != "" {
		opts.connStr = connStrCyr
	}

	opts.driver = strings.ToLower(strings.TrimSpace(opts.driver))
	if opts.driver == "postgresql" {
		opts.driver = "postgres"
	}

	opts.workers = envInt("SQL2PARQUET_WORKERS", runtime.NumCPU())
	opts.batchSize = envInt("SQL2PARQUET_BATCH_SIZE", 2048)

	return opts
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func validateOptions(opts *options) error {
	if opts.query == "" {
		return errors.New("не задана SQL-команда (-S)")
	}
	if opts.output == "" {
		return errors.New("не задан выходной файл (-o)")
	}

	// Если строка соединения не задана, требуем минимум host/port/database.
	if opts.connStr == "" {
		if opts.host == "" || opts.port <= 0 || opts.database == "" {
			return errors.New("если не задана строка соединения (-c), нужно задать -H, -P и -D")
		}
	}

	// Если драйвер не задан, пытаемся угадать по схеме connection string.
	if opts.driver == "" {
		lower := strings.ToLower(opts.connStr)
		switch {
		case strings.HasPrefix(lower, "oracle:"):
			opts.driver = "oracle"
		case strings.HasPrefix(lower, "postgres://"), strings.HasPrefix(lower, "postgresql://"):
			opts.driver = "postgres"
		default:
			return errors.New("не удалось определить драйвер; задайте -d oracle или -d postgres")
		}
	}

	if opts.driver != "oracle" && opts.driver != "postgres" {
		return errors.New("драйвер должен быть oracle или postgres")
	}

	if opts.workers <= 0 {
		opts.workers = runtime.NumCPU()
	}
	if opts.batchSize <= 0 {
		opts.batchSize = 2048
	}

	return nil
}

func openDB(opts options) (*sql.DB, error) {
	var driverName, dsn string

	switch opts.driver {
	case "postgres":
		driverName = "pgx"
		if opts.connStr != "" {
			dsn = opts.connStr
		} else {
			dsn = postgresDSN(opts)
		}
	case "oracle":
		driverName = "oracle"
		if opts.connStr != "" {
			dsn = opts.connStr
		} else {
			dsn = oracleDSN(opts)
		}
	default:
		return nil, fmt.Errorf("unsupported driver %q", opts.driver)
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	// Один запрос — одно соединение. Параллелизм обеспечивается конвейером обработки,
	// а не несколькими параллельными курсорами.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)

	return db, nil
}

func postgresDSN(opts options) string {
	u := url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", opts.host, opts.port),
		Path:   opts.database,
	}
	if opts.user != "" {
		u.User = url.UserPassword(opts.user, opts.password)
	}

	q := u.Query()
	if q.Get("sslmode") == "" {
		q.Set("sslmode", "disable")
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func oracleDSN(opts options) string {
	u := url.URL{
		Scheme: "oracle",
		Host:   fmt.Sprintf("%s:%d", opts.host, opts.port),
	}
	if opts.user != "" {
		u.User = url.UserPassword(opts.user, opts.password)
	}

	q := u.Query()
	if q.Get("prefetch_rows") == "" {
		q.Set("prefetch_rows", "1000")
	}

	db := opts.database

	// Удобство: если нужно подключиться по SID, можно передать -D SID:XE.
	if strings.HasPrefix(strings.ToUpper(db), "SID:") {
		q.Set("SID", db[4:])
		u.Path = "/"
	} else {
		u.Path = db
	}

	u.RawQuery = q.Encode()
	return u.String()
}

func export(ctx context.Context, opts options) (err error) {
	stats := newProgressState(opts.output)

	// Если нужно отключить прогресс-бар без отключения итоговой статистики,
	// можно запустить утилиту с SQL2PARQUET_PROGRESS=0.
	stopProgress := func() {}
	if os.Getenv("SQL2PARQUET_PROGRESS") != "0" {
		stopProgress = stats.run(ctx, 500*time.Millisecond)
	}

	var written int64
	var fileSize int64

	defer func() {
		stopProgress()
		stats.printSummary(written, fileSize, err)
	}()

	db, err := openDB(opts)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	rows, err := db.QueryContext(ctx, opts.query)
	if err != nil {
		return fmt.Errorf("sql query: %w", err)
	}
	defer rows.Close()

	cols, parquetSchema, err := buildSchema(rows)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return errors.New("запрос не вернул ни одной колонки")
	}

	if dir := filepath.Dir(opts.output); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir: %w", err)
		}
	}

	fw, err := local.NewLocalFileWriter(opts.output)
	if err != nil {
		return fmt.Errorf("output file: %w", err)
	}

	fileClosed := false
	defer func() {
		if !fileClosed {
			// Вызов без присваивания ошибки намеренно:
			// в разных версиях/source-обёртках Close может быть func() или func() error.
			fw.Close()
		}
	}()

	pw, err := writer.NewCSVWriter(parquetSchema, fw, int64(opts.workers))
	if err != nil {
		return fmt.Errorf("parquet writer: %w", err)
	}

	written, err = streamRows(ctx, rows, cols, pw, opts.batchSize, opts.workers, stats)
	if err != nil {
		return fmt.Errorf("export: %w", err)
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("finalize parquet writer: %w", err)
	}

	fileClosed = true
	fw.Close()

	if fi, statErr := os.Stat(opts.output); statErr == nil {
		fileSize = fi.Size()
	}

	return nil
}

func buildSchema(rows *sql.Rows) ([]columnDef, []string, error) {
	used := make(map[string]bool)

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		// Fallback на случай, если драйвер не отдал типы.
		names, err := rows.Columns()
		if err != nil {
			return nil, nil, err
		}

		cols := make([]columnDef, len(names))
		schema := make([]string, len(names))

		for i, n := range names {
			name := uniqueName(sanitizeName(n, i), used)
			cols[i] = columnDef{Name: name, Target: targetString}
			schema[i] = schemaEntry(name, targetString)
		}

		return cols, schema, nil
	}

	cols := make([]columnDef, len(colTypes))
	schema := make([]string, len(colTypes))

	for i, ct := range colTypes {
		name := uniqueName(sanitizeName(ct.Name(), i), used)
		target := chooseTarget(ct)

		cols[i] = columnDef{Name: name, Target: target}
		schema[i] = schemaEntry(name, target)
	}

	return cols, schema, nil
}

func sanitizeName(name string, idx int) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Sprintf("col_%d", idx+1)
	}

	var b strings.Builder
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}

	s := b.String()
	if s == "" {
		s = fmt.Sprintf("col_%d", idx+1)
	}

	// Parquet-схема через parquet-go хуже работает с именами, начинающимися с цифры.
	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}

	return s
}

func uniqueName(base string, used map[string]bool) string {
	key := strings.ToLower(base)
	if !used[key] {
		used[key] = true
		return base
	}

	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s_%d", base, i)
		key = strings.ToLower(candidate)
		if !used[key] {
			used[key] = true
			return candidate
		}
	}
}

func schemaEntry(name string, target targetKind) string {
	switch target {
	case targetInt64:
		return fmt.Sprintf("name=%s, type=INT64, repetitiontype=OPTIONAL", name)
	case targetDouble:
		return fmt.Sprintf("name=%s, type=DOUBLE, repetitiontype=OPTIONAL", name)
	case targetBool:
		return fmt.Sprintf("name=%s, type=BOOLEAN, repetitiontype=OPTIONAL", name)
	case targetBytes:
		return fmt.Sprintf("name=%s, type=BYTE_ARRAY, repetitiontype=OPTIONAL", name)
	default:
		return fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", name)
	}
}

func chooseTarget(ct *sql.ColumnType) targetKind {
	t := strings.ToUpper(strings.TrimSpace(ct.DatabaseTypeName()))
	if t == "" {
		return targetString
	}

	if isBoolType(t) {
		return targetBool
	}
	if isBinaryType(t) {
		return targetBytes
	}
	if isIntegerType(t) {
		return targetInt64
	}
	if isFloatType(t) {
		return targetDouble
	}

	if isNumericType(t) {
		prec, scale, ok := ct.DecimalSize()
		if ok {
			// Целые NUMBER/NUMERIC с разумной точностью храним как INT64.
			if scale == 0 && prec > 0 && prec <= 18 {
				return targetInt64
			}
			// Небольшие десятичные значения храним как DOUBLE.
			// Если важна точная фиксированная точность, лучше менять на DECIMAL/BYTE_ARRAY.
			if prec > 0 && prec <= 15 {
				return targetDouble
			}
		}

		// Крупные/неизвестные NUMERIC/NUMBER сохраняем строкой, чтобы не терять точность.
		return targetString
	}

	// Даты, времена, JSON, UUID, CLOB и всё остальное сохраняем как UTF8-строку.
	return targetString
}

func isBoolType(t string) bool {
	if t == "BOOL" || t == "BOOLEAN" {
		return true
	}
	return strings.Contains(t, "BOOL")
}

func isBinaryType(t string) bool {
	switch t {
	case "BYTEA", "BLOB", "BINARY", "VARBINARY", "RAW", "LONG RAW", "BFILE", "IMAGE":
		return true
	}
	return strings.Contains(t, "BINARY") ||
		strings.Contains(t, "BLOB") ||
		strings.Contains(t, "RAW") ||
		strings.Contains(t, "BYTEA")
}

func isIntegerType(t string) bool {
	switch t {
	case "INT", "INTEGER", "INT2", "INT4", "INT8",
		"SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",
		"SERIAL", "BIGSERIAL", "SMALLSERIAL":
		return true
	}

	// Осторожно: не принимаем INTERVAL и похожие типы.
	if strings.Contains(t, "INT") &&
		!strings.Contains(t, "INTERVAL") &&
		!strings.Contains(t, "TIMESTAMP") &&
		!strings.Contains(t, "POINT") {
		return true
	}

	return false
}

func isFloatType(t string) bool {
	switch t {
	case "FLOAT", "FLOAT4", "FLOAT8", "REAL", "DOUBLE", "DOUBLE PRECISION",
		"BINARY_FLOAT", "BINARY_DOUBLE":
		return true
	}
	return strings.Contains(t, "FLOAT") ||
		strings.Contains(t, "DOUBLE") ||
		strings.Contains(t, "REAL")
}

func isNumericType(t string) bool {
	switch t {
	case "NUMERIC", "DECIMAL", "NUMBER", "DEC", "FIXED", "MONEY", "SMALLMONEY":
		return true
	}
	return strings.Contains(t, "NUMERIC") ||
		strings.Contains(t, "DECIMAL") ||
		strings.Contains(t, "NUMBER")
}

func streamRows(
	ctx context.Context,
	rows *sql.Rows,
	cols []columnDef,
	pw *writer.CSVWriter,
	batchSize int,
	workers int,
	stats *progressState,
) (int64, error) {
	if batchSize <= 0 {
		batchSize = 2048
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	batches := make(chan [][]any, 4)
	readErrCh := make(chan error, 1)

	// Поток чтения из БД: читает пачками, не держит весь результат в памяти.
	go func() {
		defer close(batches)

		batch := make([][]any, 0, batchSize)

		for rows.Next() {
			if ctx.Err() != nil {
				readErrCh <- ctx.Err()
				return
			}

			raw := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range raw {
				ptrs[i] = &raw[i]
			}

			if err := rows.Scan(ptrs...); err != nil {
				readErrCh <- fmt.Errorf("scan: %w", err)
				return
			}

			// Некоторые драйверы могут отдавать []byte, валидные только до следующего Scan.
			// Копируем такие значения сразу.
			copyBytes(raw)

			batch = append(batch, raw)

			if len(batch) >= batchSize {
				select {
				case batches <- batch:
				case <-ctx.Done():
					readErrCh <- ctx.Err()
					return
				}
				// Обязательно новый срез, чтобы не переиспользовать память,
				// на которую ещё может ссылаться отправленная пачка.
				batch = make([][]any, 0, batchSize)
			}
		}

		if err := rows.Err(); err != nil {
			readErrCh <- fmt.Errorf("rows: %w", err)
			return
		}

		if len(batch) > 0 {
			select {
			case batches <- batch:
			case <-ctx.Done():
				readErrCh <- ctx.Err()
				return
			}
		}
	}()

	var written int64

	// Обработка пачек: пока writer пишет одну пачку, reader может читать следующую.
	for batch := range batches {
		select {
		case err := <-readErrCh:
			return written, err
		default:
		}

		recs, err := convertBatch(ctx, batch, cols, workers)
		if err != nil {
			return written, err
		}

		for _, rec := range recs {
			if err := pw.Write(rec); err != nil {
				return written, fmt.Errorf("parquet write: %w", err)
			}
			written++

			if stats != nil {
				stats.addRows(1)
			}
		}

		if ctx.Err() != nil {
			return written, ctx.Err()
		}
	}

	select {
	case err := <-readErrCh:
		return written, err
	default:
		return written, nil
	}
}

func copyBytes(row []any) {
	for i, v := range row {
		if b, ok := v.([]byte); ok {
			cp := make([]byte, len(b))
			copy(cp, b)
			row[i] = cp
		}
	}
}

func convertBatch(
	ctx context.Context,
	batch [][]any,
	cols []columnDef,
	workers int,
) ([][]interface{}, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if workers < 1 {
		workers = 1
	}
	if workers > len(batch) {
		workers = len(batch)
	}

	res := make([][]interface{}, len(batch))
	idxCh := make(chan int, len(batch))
	for i := range batch {
		idxCh <- i
	}
	close(idxCh)

	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range idxCh {
				if ctx.Err() != nil {
					return
				}

				rec, err := convertRow(batch[i], cols)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("row %d: %w", i, err):
					default:
					}
					cancel()
					return
				}

				res[i] = rec
			}
		}()
	}

	wg.Wait()

	if ctx.Err() != nil {
		select {
		case err := <-errCh:
			return nil, err
		default:
			return nil, ctx.Err()
		}
	}

	select {
	case err := <-errCh:
		return nil, err
	default:
		return res, nil
	}
}

func convertRow(raw []any, cols []columnDef) ([]interface{}, error) {
	rec := make([]interface{}, len(cols))

	for i, c := range cols {
		v, err := convertValue(raw[i], c.Target)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.Name, err)
		}
		rec[i] = v
	}

	return rec, nil
}

func convertValue(v any, target targetKind) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch target {
	case targetInt64:
		return toInt64(v)
	case targetDouble:
		return toFloat64(v)
	case targetBool:
		return toBool(v)
	case targetBytes:
		return toBytes(v)
	default:
		return toString(v)
	}
}

func toInt64(v any) (interface{}, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case uint:
		return uintToInt64(uint64(x))
	case uint64:
		return uintToInt64(x)
	case uint32:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case float64:
		return floatToInt64(x)
	case float32:
		return floatToInt64(float64(x))
	case bool:
		if x {
			return int64(1), nil
		}
		return int64(0), nil
	case string:
		return parseInt64String(x)
	case []byte:
		return parseInt64String(string(x))
	case time.Time:
		return x.Unix(), nil
	default:
		return parseInt64String(stringify(v))
	}
}

func parseInt64String(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}

	// На случай значений вида 123.0.
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return floatToInt64(f)
	}

	return nil, fmt.Errorf("невозможно преобразовать %q в INT64", s)
}

func uintToInt64(v uint64) (interface{}, error) {
	if v > math.MaxInt64 {
		return nil, fmt.Errorf("uint64 value %d does not fit int64", v)
	}
	return int64(v), nil
}

func floatToInt64(f float64) (interface{}, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, errors.New("невозможно преобразовать NaN/Inf в INT64")
	}
	if f < math.MinInt64 || f > math.MaxInt64 {
		return nil, fmt.Errorf("float value %g does not fit int64", f)
	}
	return int64(f), nil
}

func toFloat64(v any) (interface{}, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int16:
		return float64(x), nil
	case int8:
		return float64(x), nil
	case uint:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case uint16:
		return float64(x), nil
	case uint8:
		return float64(x), nil
	case bool:
		if x {
			return float64(1), nil
		}
		return float64(0), nil
	case string:
		return parseFloat64String(x)
	case []byte:
		return parseFloat64String(string(x))
	case time.Time:
		return float64(x.UnixNano()), nil
	default:
		return parseFloat64String(stringify(v))
	}
}

func parseFloat64String(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("невозможно преобразовать %q в DOUBLE", s)
	}

	return f, nil
}

func toBool(v any) (interface{}, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int64:
		return x != 0, nil
	case int:
		return x != 0, nil
	case int32:
		return x != 0, nil
	case int16:
		return x != 0, nil
	case int8:
		return x != 0, nil
	case uint:
		return x != 0, nil
	case uint64:
		return x != 0, nil
	case uint32:
		return x != 0, nil
	case uint16:
		return x != 0, nil
	case uint8:
		return x != 0, nil
	case float64:
		return x != 0, nil
	case float32:
		return x != 0, nil
	case string:
		return parseBoolString(x)
	case []byte:
		return parseBoolString(string(x))
	default:
		return parseBoolString(stringify(v))
	}
}

func parseBoolString(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	l := strings.ToLower(s)
	switch l {
	case "null", "nil":
		return nil, nil
	case "1", "t", "true", "y", "yes", "on":
		return true, nil
	case "0", "f", "false", "n", "no", "off":
		return false, nil
	}

	b, err := strconv.ParseBool(s)
	if err == nil {
		return b, nil
	}

	return nil, fmt.Errorf("невозможно преобразовать %q в BOOLEAN", s)
}

func toString(v any) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	return stringify(v), nil
}

func toBytes(v any) (interface{}, error) {
	switch x := v.(type) {
	case []byte:
		cp := make([]byte, len(x))
		copy(cp, x)
		return cp, nil
	case string:
		return []byte(x), nil
	case time.Time:
		return []byte(x.Format(time.RFC3339Nano)), nil
	default:
		return []byte(stringify(v)), nil
	}
}

func stringify(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case *time.Time:
		if x != nil {
			return x.Format(time.RFC3339Nano)
		}
		return ""
	case fmt.Stringer:
		return x.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

type progressState struct {
	rows   atomic.Int64
	start  time.Time
	output string
}

func newProgressState(output string) *progressState {
	return &progressState{
		start:  time.Now(),
		output: output,
	}
}

func (p *progressState) addRows(n int64) {
	if n > 0 {
		p.rows.Add(n)
	}
}

func (p *progressState) currentFileSize() int64 {
	fi, err := os.Stat(p.output)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func (p *progressState) run(ctx context.Context, interval time.Duration) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.render()
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

func (p *progressState) render() {
	elapsed := time.Since(p.start)

	sec := elapsed.Seconds()
	if sec < 0.1 {
		sec = 0.1
	}

	rows := p.rows.Load()
	bytes := p.currentFileSize()

	rowsPerSec := int64(float64(rows) / sec)
	bytesPerSec := int64(float64(bytes) / sec)

	spinner := []string{"|", "/", "-", "\\"}
	idx := int(elapsed.Milliseconds()/250) % len(spinner)

	line := fmt.Sprintf(
		"%s время=%s строк=%d файл=%s скорость=%s/s строк/s=%d",
		spinner[idx],
		elapsed.Truncate(time.Second),
		rows,
		humanBytes(bytes),
		humanBytes(bytesPerSec),
		rowsPerSec,
	)

	// \r возвращает каретку в начало строки, %-160s затирает хвост старой строки.
	fmt.Fprintf(os.Stderr, "\r%-160s", line)
}

func (p *progressState) printSummary(written int64, fileSize int64, exportErr error) {
	elapsed := time.Since(p.start)

	sec := elapsed.Seconds()
	if sec < 0.001 {
		sec = 0.001
	}

	rows := written
	if rows == 0 {
		rows = p.rows.Load()
	}

	bytes := fileSize
	if bytes <= 0 {
		bytes = p.currentFileSize()
	}

	// Завершаем строку прогресса.
	fmt.Fprintln(os.Stderr)

	if exportErr != nil {
		fmt.Fprintf(
			os.Stderr,
			"экспорт остановлен: время=%s строк=%d записано=%s\n",
			elapsed.Truncate(time.Second),
			rows,
			humanBytes(bytes),
		)
		return
	}

	rowsPerSec := int64(float64(rows) / sec)
	bytesPerSec := int64(float64(bytes) / sec)

	fmt.Fprintf(
		os.Stderr,
		"готово: время=%s строк=%d файл=%s размер=%s средняя скорость=%s/s средняя строк/s=%d\n",
		elapsed.Truncate(time.Second),
		rows,
		p.output,
		humanBytes(bytes),
		humanBytes(bytesPerSec),
		rowsPerSec,
	)
}

func humanBytes(n int64) string {
	const base = 1024

	if n < base {
		return fmt.Sprintf("%d B", n)
	}

	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}

	f := float64(n)
	i := -1

	for f >= base && i < len(units)-1 {
		f /= base
		i++
	}

	return fmt.Sprintf("%.2f %s", f, units[i])
}
