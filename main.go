package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// 配置项
const (
	DBPath           = "stock_data.duckdb"
	PathDailyMetrics = "C:\\baidunetdiskdownload\\每日指标\\*.csv"
	PathTechFactors  = "C:\\baidunetdiskdownload\\技术因子_复权数据\\*.csv"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println(">>> 启动 Mallard 数据导入程序...")

	// 1. 初始化数据库
	// ==========================================
	db, err := sql.Open("duckdb", DBPath)
	if err != nil {
		log.Fatalf("错误: 无法打开数据库 %s: %v", DBPath, err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatalf("错误: 数据库连接失败: %v", err)
	}

	// 设定内存上限，防止大数据量下 OOM (Out Of Memory)
	execSQL(db, "SET memory_limit='8GB'")

	// 2. 创建 Staging 视图 (显式类型转换)
	// ==========================================
	// 我们不信任 CSV 的自动推断，必须手动 CAST 关键字段
	// 且使用 strptime 将 '20230101' 字符串转为标准 DATE 类型

	log.Println(">>> 正在读取 CSV 并构建 Staging 视图...")

	// A. 每日指标视图 (v_daily_metrics)
	execSQL(db, fmt.Sprintf(`
		CREATE OR REPLACE VIEW v_daily_metrics AS 
		SELECT 
			CAST("股票代码" AS VARCHAR) AS symbol,
			strptime(CAST("交易日期" AS VARCHAR), '%%Y%%m%%d') AS date,
			CAST("市盈率TTM" AS DOUBLE) AS pe_ttm,
			CAST("市净率" AS DOUBLE) AS pb,
			CAST("总市值(万元)" AS DOUBLE) AS market_cap,
			CAST("换手率" AS DOUBLE) AS turnover_rate
		FROM read_csv('%s', header=true, auto_detect=true);
	`, PathDailyMetrics))

	// B. 技术因子视图 (v_tech_factors)
	execSQL(db, fmt.Sprintf(`
		CREATE OR REPLACE VIEW v_tech_factors AS 
		SELECT 
			CAST("股票代码" AS VARCHAR) AS symbol,
			strptime(CAST("交易日期" AS VARCHAR), '%%Y%%m%%d') AS date,
			CAST("开盘价" AS DOUBLE) AS open,
			CAST("最高价" AS DOUBLE) AS high,
			CAST("最低价" AS DOUBLE) AS low,
			CAST("收盘价" AS DOUBLE) AS close,
			CAST("成交量(手)" AS DOUBLE) AS volume,
			CAST("成交额(千元)" AS DOUBLE) AS amount,
			CAST("复权因子" AS DOUBLE) AS adj_factor,
			CAST("MACD" AS DOUBLE) AS macd,
			CAST("RSI_6" AS DOUBLE) AS rsi_6
		FROM read_csv('%s', header=true, auto_detect=true);
	`, PathTechFactors))

	// 3. 核心转换：清洗、去重与合并 (ELT)
	// ==========================================
	log.Println(">>> 正在执行数据合并与清洗 (JOIN & DISTINCT)...")
	start := time.Now()

	// 重建目标表
	execSQL(db, "DROP TABLE IF EXISTS stock_history")

	// 逻辑说明：
	// 1. DISTINCT: 防止 CSV 文件级重复
	// 2. INNER JOIN: 确保只有当 "技术面" 和 "基本面" 数据在同一天都存在时才入库 (强一致性)
	execSQL(db, `
		CREATE TABLE stock_history AS 
		SELECT DISTINCT
			t.symbol,
			t.date,
			-- 技术面数据
			t.open, t.high, t.low, t.close,
			t.volume, t.amount, t.adj_factor,
			t.macd, t.rsi_6,
			-- 基本面数据
			d.pe_ttm, d.pb, d.market_cap, d.turnover_rate
		FROM v_tech_factors t
		INNER JOIN v_daily_metrics d 
			ON t.symbol = d.symbol 
			AND t.date = d.date;
	`)

	log.Printf(">>> 数据处理完成，耗时: %s", time.Since(start))

	// 4. 数据审计 (Data Audit)
	// ==========================================
	auditDataQuality(db)

	// 5. 创建索引
	// ==========================================
	log.Println(">>> 正在创建索引...")
	execSQL(db, "CREATE INDEX idx_stock_history_symbol_date ON stock_history(symbol, date)")

	log.Println(">>> ✅ Mallard 任务全部完成")
}

// 辅助函数：执行 SQL 并处理错误
func execSQL(db *sql.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("[FATAL] SQL 执行失败: %v\nQuery: %s", err, query)
	}
}

// 辅助函数：统计行数
func getCount(db *sql.DB, tableName string) int64 {
	var count int64
	err := db.QueryRow("SELECT COUNT(*) FROM " + tableName).Scan(&count)
	if err != nil {
		log.Fatalf("[FATAL] 无法统计表 %s: %v", tableName, err)
	}
	return count
}

// 数据审计逻辑
func auditDataQuality(db *sql.DB) {
	log.Println(">>> 开始数据审计...")

	cntDaily := getCount(db, "v_daily_metrics")
	cntTech := getCount(db, "v_tech_factors")
	cntFinal := getCount(db, "stock_history")

	fmt.Println("\n============= 数据审计报告 =============")
	fmt.Printf("1. [源] 每日指标行数 : %d\n", cntDaily)
	fmt.Printf("2. [源] 技术因子行数 : %d\n", cntTech)
	fmt.Printf("3. [终] 最终入库行数 : %d\n", cntFinal)
	fmt.Println("========================================")

	// 简单校验：如果最终数据量远小于源数据，发出警告
	minSource := cntDaily
	if cntTech < cntDaily {
		minSource = cntTech
	}

	if minSource > 0 {
		rate := float64(cntFinal) / float64(minSource)
		fmt.Printf("数据匹配率: %.2f%%\n", rate*100)
		if rate < 0.9 {
			log.Println("[WARN] 警告：最终数据量比源数据少 10% 以上！请检查日期对齐或数据完整性。")
		} else {
			log.Println("[INFO] 数据完整性校验通过。")
		}
	} else {
		log.Println("[WARN] 源数据为空！")
	}
	fmt.Println()
}
