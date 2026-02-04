package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	DBPath = "stock_data.db"
	// 请确保路径没有多余空格
	PathTechFactors  = "C:\\baidunetdiskdownload\\技术因子_复权数据\\*.csv"
	PathDailyMetrics = "C:\\baidunetdiskdownload\\每日指标\\*.csv"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	startTotal := time.Now()
	log.Println(">>> 启动全自动量化数据清洗程序 (v2.1 - 智能分隔符版)...")

	os.Remove(DBPath)
	db, err := sql.Open("sqlite", DBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 性能配置
	mustExec(db, "PRAGMA journal_mode = WAL;")
	mustExec(db, "PRAGMA synchronous = OFF;")
	mustExec(db, "PRAGMA temp_store = MEMORY;")

	createTables(db)

	// ---------------------------------------------------------
	// 1. 导入技术因子 (提取复权价)
	// ---------------------------------------------------------
	// 索引：0:代码, 1:日期, 2:收盘(原), 12:开(后), 14:收(后), 16:高(后), 18:低(后)
	importCSV(db, PathTechFactors, "staging_tech", 19, func(record []string) []any {
		if len(record) < 19 {
			return nil
		}
		return []any{
			record[0],  // symbol
			record[1],  // date
			record[2],  // close_raw
			record[14], // close_adj
			record[12], // open_adj
			record[16], // high_adj
			record[18], // low_adj
		}
	})

	// ---------------------------------------------------------
	// 2. 导入每日指标 (提取 PE)
	// ---------------------------------------------------------
	// 索引：0:代码, 1:日期, 14:市盈率
	// 注意：如果导入仍为0，程序会打印第一行的解析情况帮助调试
	importCSV(db, PathDailyMetrics, "staging_daily", 15, func(record []string) []any {
		if len(record) < 15 {
			return nil
		}
		return []any{
			record[0],  // symbol
			record[1],  // date
			record[14], // pe
		}
	})

	// ---------------------------------------------------------
	// 3. 建立索引 & 合并数据
	// ---------------------------------------------------------
	log.Println(">>> 正在优化临时索引...")
	mustExec(db, "CREATE INDEX idx_st_tech_sd ON staging_tech(symbol, date);")
	mustExec(db, "CREATE INDEX idx_st_daily_sd ON staging_daily(symbol, date);")

	log.Println(">>> 正在执行最终合并与数据清洗...")
	eltQuery := `
	INSERT INTO stock_history 
	SELECT 
		t.symbol,
		-- 日期格式化: 19910404 -> 1991-04-04
		substr(t.date, 1, 4) || '-' || substr(t.date, 5, 2) || '-' || substr(t.date, 7, 2),
		
		CAST(t.close_raw AS REAL),
		CAST(t.close_adj AS REAL),
		CAST(t.open_adj AS REAL),
		CAST(t.high_adj AS REAL),
		CAST(t.low_adj AS REAL),

		-- 清洗 PE: 去除空格，空字符串转 NULL
		CAST(NULLIF(trim(d.pe), '') AS REAL)

	FROM staging_tech t
	INNER JOIN staging_daily d 
		ON t.symbol = d.symbol 
		AND t.date = d.date;
	`
	mustExec(db, "BEGIN TRANSACTION;")
	mustExec(db, eltQuery)
	mustExec(db, "COMMIT;")

	// ---------------------------------------------------------
	// 4. 收尾
	// ---------------------------------------------------------
	log.Println(">>> 正在清理临时空间...")
	mustExec(db, "DROP TABLE staging_tech;")
	mustExec(db, "DROP TABLE staging_daily;")
	mustExec(db, "VACUUM;")

	log.Printf(">>> ✅ 任务全部完成! 耗时: %s", time.Since(startTotal))

	// 最终自检
	checkCount(db)
}

// ---------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------

func createTables(db *sql.DB) {
	mustExec(db, `CREATE TABLE staging_tech (
		symbol TEXT, date TEXT, close_raw TEXT, 
		close_adj TEXT, open_adj TEXT, high_adj TEXT, low_adj TEXT
	);`)

	mustExec(db, `CREATE TABLE staging_daily (
		symbol TEXT, date TEXT, pe TEXT
	);`)

	mustExec(db, `CREATE TABLE stock_history (
		symbol      TEXT NOT NULL,
		date        TEXT NOT NULL,
		close       REAL, 
		close_adj   REAL, 
		open_adj    REAL, 
		high_adj    REAL, 
		low_adj     REAL, 
		pe          REAL, 
		PRIMARY KEY (symbol, date)
	) WITHOUT ROWID, STRICT;`)
}

// 智能 CSV 导入器 (自动识别逗号或Tab)
func importCSV(db *sql.DB, pattern string, tableName string, minCols int, mapper func([]string) []any) {
	files, _ := filepath.Glob(pattern)
	if len(files) == 0 {
		log.Printf("[ERROR] 未找到文件: %s", pattern)
		return
	}

	tx, _ := db.Begin()
	var stmt *sql.Stmt

	rowCount := 0
	filesCount := 0

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}

		// --- 智能探测分隔符 ---
		// 先读取第一行文本，看看哪个分隔符多
		scanner := bufio.NewScanner(f)
		var comma rune = ',' // 默认逗号
		if scanner.Scan() {
			line := scanner.Text()
			// 如果包含制表符，且比逗号多，或者是包含制表符且没有逗号
			if strings.Count(line, "\t") > strings.Count(line, ",") {
				comma = '\t'
			}
		}
		f.Seek(0, 0) // 探测完必须回到文件开头

		r := csv.NewReader(f)
		r.Comma = comma // 设置检测到的分隔符
		r.LazyQuotes = true

		// 跳过 Header
		_, err = r.Read()
		if err != nil {
			f.Close()
			continue
		}

		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}

			// 调试日志：如果总是跳过，打印第一条失败的原因
			if len(record) < minCols {
				if rowCount == 0 && filesCount == 0 {
					log.Printf("[DEBUG] 首行解析失败! 检测分隔符: '%c', 解析后列数: %d (需要: %d), 内容: %v",
						comma, len(record), minCols, record)
				}
				continue
			}

			args := mapper(record)
			if args == nil {
				continue
			}

			if stmt == nil {
				placeholders := strings.Repeat("?,", len(args))
				placeholders = placeholders[:len(placeholders)-1]
				query := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, placeholders)
				stmt, err = tx.Prepare(query)
				if err != nil {
					log.Fatal(err)
				}
			}

			stmt.Exec(args...)
			rowCount++
		}
		f.Close()
		fmt.Printf(".")
		filesCount++
	}
	if stmt != nil {
		stmt.Close()
	}
	tx.Commit()
	fmt.Printf("\n>>> %s 导入完成: %d 行\n", tableName, rowCount)
}

func mustExec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		log.Fatalf("SQL Error: %v | Query: %s", err, query)
	}
}

func checkCount(db *sql.DB) {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM stock_history").Scan(&count)
	log.Printf(">>> 最终入库总行数: %d", count)

	var nullPe int
	db.QueryRow("SELECT COUNT(*) FROM stock_history WHERE pe IS NULL").Scan(&nullPe)
	log.Printf(">>> 其中 PE 为 NULL (亏损/缺失) 的行数: %d", nullPe)
}
