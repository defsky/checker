package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/defsky/dmon/config"
	"github.com/defsky/dmon/db"
)

// ProcessStatus ...
type ProcessStatus int

const (
	// WantHeader ...
	WantHeader ProcessStatus = iota

	// WantData ...
	WantData

	// FoundDataEnd ...
	FoundDataEnd
)

var excelFile, headFile string
var needArchive, needPrintSrcData bool

func main() {
	flag.StringVar(&excelFile, "d", "", "data file with excel format(*.xlsx)")
	// flag.StringVar(&headFile, "h", "", "header defination file in Comma-Separated Values")
	flag.BoolVar(&needArchive, "a", false, "archiving flag, archive the source data to history db or not")
	flag.BoolVar(&needPrintSrcData, "p", false, "print flag, print source data or not")
	flag.Parse()

	log.Println("data checker v1.0")
	config.Init()

	headFile = config.GetConfig().Target
	if len(headFile) <= 0 {
		log.Fatalln("need header defination in config.yml")
	}
	if len(excelFile) <= 0 {
		log.Fatalln("need data file with excel format(*.xlsx)")
	}

	// header, end := GetHeader(headFile)
	header, end := GetHeaders(headFile)

	if len(header) > 0 {
		log.Printf("表头定义：%v, 结束标记: %s", header, end)

	}

	data := LoadData(excelFile, header, end, needPrintSrcData)

	rowCount := len(data.Data)
	if rowCount <= 0 {
		log.Fatalln("没有找到和表头格式匹配的数据")
	}
	log.Printf("已加载数据行数：%d", rowCount)

	report := make([]string, 0)

	log.Println("检查重复项 ...")
	soDocNoColName := ""
	for k, v := range data.Header {
		if k == 1 {
			soDocNoColName = v
			break
		}
	}
	//soDoc, err := data.GetColumnByName("销售订单号")
	soDoc, err := data.GetColumnByName(soDocNoColName)
	if err != nil {
		log.Println(err)
	}
	newSoDoc, dump := deDumplicate(soDoc)
	hasDump := false
	for k, v := range dump {
		hasDump = true
		msg := fmt.Sprintf("发现重复项：%s , 数量: %d", k, v)
		report = append(report, msg)
		log.Println(msg)
	}
	if hasDump {
		soDoc = newSoDoc
	}

	log.Println("初始化数据库连接 ...")
	db.Init()

	storedb := db.Mssql("store")
	if err := storedb.DB().Ping(); err != nil {
		log.Fatalf("数据库连接错误: %s", err)
	}
	// storedb.ShowSQL(true)
	// storedb.SetLogLevel(xormlog.LOG_DEBUG)

	soDocQuery := storedb.SQL(`
		SELECT Sa.OrderNumber,Sa.TotalNumber,Sa.U9ARBillCode,Sa.ClosedBy,SysUser.UserName,Sh.ShopName
		FROM sales.SalesOrder Sa
		LEFT JOIN basedata.Shop Sh ON Sa.ShopId=Sh.ShopId
		LEFT JOIN Admin.SystemUser SysUser ON Sa.ClosedBy=SysUser.SystemUserId
		WHERE (
			Sa.SalesOrderType=8
			OR (Sa.SalesOrderType=1 AND Sa.SalesType=8)
			OR SalesType=127
		)
		and Sa.OrderNumber in (` + GetSQLValueList(soDoc) + `)`)

	log.Println("查询需要核对的订单信息 ...")
	srcDoc, err := soDocQuery.QueryString()
	if err != nil {
		sqlText, _ := soDocQuery.LastSQL()
		log.Fatalf("数据库查询错误：%s", err.Error()+"\n"+sqlText)
	}
	srcCount := len(srcDoc)
	log.Printf("找到源单据条数：%d", srcCount)

	if srcCount != rowCount {
		msg := fmt.Sprintf("找到的单据数和源数据不匹配: db(%d), src(%d)", len(srcDoc), rowCount)
		report = append(report, msg)
		log.Printf(msg)
	}

	log.Println("搜索未终止的订单 ...")
	lotCodes := make([][]string, 0)

	for _, v := range soDoc {
		for _, r := range srcDoc {
			if r["OrderNumber"] == v {
				if len(r["ClosedBy"]) <= 0 {
					msg := fmt.Sprintf("发现未终止的订单：%s", v)
					report = append(report, msg)
					log.Println(msg)
				}
				lot := r["TotalNumber"]
				if len(lot) > 0 {
					lotCodes = append(lotCodes, []string{r["OrderNumber"], lot})
				}
			}
		}
	}

	u9db := db.Mssql("u928")

	if len(lotCodes) > 0 {
		lotCodeList := make([]string, 1)
		for _, row := range lotCodes {
			lotCodeList = append(lotCodeList, row[1])
		}
		log.Printf("找到总编号：%v", lotCodeList)

		log.Println("在U9系统查询总编号是否存在 ...")
		if err := u9db.DB().Ping(); err != nil {
			log.Fatalf("数据库连接错误: %s", err)
		}
		lotQuery := u9db.SQL(`select LotCode from Lot_LotMaster where LotCode in (` + GetSQLValueList(lotCodeList) + `)`)
		srcLot, err := lotQuery.QueryString()
		if err != nil {
			log.Fatalf("数据库查询错误：%s", err)
		}
		if len(srcLot) > 0 {
			for _, r := range srcLot {
				srcLotCode := r["LotCode"]
				soFound := false
				for _, row := range lotCodes {
					if row[1] == srcLotCode {
						msg := fmt.Sprintf("在U9系统中找到总编号: %s, 所属订单号:%s", row[1], row[0])
						report = append(report, msg)
						log.Println(msg)
						soFound = true
						break
					}
				}
				if !soFound {
					msg := fmt.Sprintf("在U9系统中找到总编号: %s, 所属订单号:%s", srcLotCode, "<未知>")
					report = append(report, msg)
					log.Printf(msg)
				}
			}
		}
	} else {
		log.Println("没有关联的总编号")
	}

	log.Println("在U9系统查询应收单 ...")
	docList := make([]string, 0)
	for _, v := range soDoc {
		docList = append(docList, "SHAR-"+v)
	}
	arQuery := u9db.SQL(`
		select a.DocNo,sum(b.ARFCMoney_TotalMoney) as 'TotalMoney'
		from AR_ARBillHead a
			inner join AR_ARBillLine b on b.ARBillHead= a.ID
		where a.DocNo in (` + GetSQLValueList(docList) + `)
		group by a.DocNo`)
	srcAR, err := arQuery.QueryString()
	if err != nil {
		log.Fatalf("数据库查询错误：%s", err)
	}

	log.Println("核对各单应收明细金额，并计算总金额 ...")
	totalMoney := 0.0
	for _, arRow := range srcAR {
		m, err := strconv.ParseFloat(arRow["TotalMoney"], 64)
		if err != nil {
			msg := fmt.Sprintf("应收金额格式错误：%s", err)
			report = append(report, msg)
			log.Println(msg)
		} else {
			totalMoney += m
		}
		found := false
		for _, datarow := range data.Data {
			reqMoney, reqSODoc := datarow[0], datarow[1]
			if strings.Contains(arRow["DocNo"], reqSODoc) {
				found = true
				if rm, er := strconv.ParseFloat(reqMoney, 64); err != nil {
					msg := fmt.Sprintf("来源金额格式不正确：%s", er)
					report = append(report, msg)
					log.Printf(msg)
				} else {
					if rm != m {
						msg := fmt.Sprintf("定金应收金额与源数据金额不一致：%s\t(%f),\t%s\t(%f)", arRow["DocNo"], m, reqSODoc, rm)
						report = append(report, msg)
						log.Println(msg)
					}
				}
			}
		}
		if !found {
			msg := fmt.Sprintf("没有找到应收单对应的来源订单号: %s", arRow["DocNo"])
			report = append(report, msg)
			log.Print(msg)
		}
	}
	msg := fmt.Sprintf("应收总金额为: %f", totalMoney)
	report = append(report, msg)
	log.Print(msg)

	log.Println("检查历史归档中是否存在源数据 ...")
	testdb := db.Mssql("test")
	histQuery := testdb.SQL(`select DocNo,TotalMoney from Store_DepositReturn where DocNo in (` + GetSQLValueList(soDoc) + `)`)
	histDoc, err := histQuery.QueryString()
	if err != nil {
		log.Fatalf("数据库查询错误：%s", err)
	}
	if len(histDoc) > 0 {
		msg := fmt.Sprintf("发现已归档过的单据 %d 个", len(histDoc))
		report = append(report, msg)

		log.Printf("发现已归档的单据 %d 个,明细如下：", len(histDoc))
		for _, row := range histDoc {
			log.Printf("\t%s\t(%s)", row["DocNo"], row["TotalMoney"])
		}
	} else {
		log.Println("在历史归档中没有发现当前源数据")
	}

	if needArchive {
		log.Println("归档当前源数据 ...")
		values := ""
		for _, v := range soDoc {
			hasHist := false
			for _, rr := range histDoc {
				if rr["DocNo"] == v {
					hasHist = true
					break
				}
			}
			if hasHist {
				log.Printf("跳过历史归档中已存在的单据: %s", v)
				continue
			}
			for _, sd := range data.Data {
				if sd[1] == v {
					if len(values) > 0 {
						values += ","
					}
					values += "(" + sd[0] + ",'" + sd[1] + "')"
					break
				}
			}
		}
		if len(values) > 0 {
			histArchive := testdb.SQL(`INSERT INTO [dbo].[Store_DepositReturn] ([TotalMoney],[DocNo]) VALUES ` + values)
			res, err := histArchive.Execute()
			if err != nil {
				log.Printf("写入归档库时出错：%s", err)
			} else {
				affected, err := res.RowsAffected()
				if err != nil {
					log.Printf("获取归档执行结果错误：%s", err)
				} else {
					log.Printf("已归档数据条数: %d", affected)
				}
			}
		} else {
			log.Printf("历史归档中已存在所有数据，没有需要归档的数据")
		}
	} else {
		log.Printf("不需要归档，处理结束")
	}

	if len(report) > 0 {
		fmt.Println("\n数据检查报告\n---------------------------------------------------------------------------------------")
		for i, v := range report {
			fmt.Printf("\t%d\t%s\n", i+1, v)
		}
	}
}

// GetHeaders ...
func GetHeaders(s string) ([]string, string) {
	e := strings.Split(s, ":")
	if len(e) < 2 {
		log.Fatalln("表头定义中没有找到数据结束标记")
	}
	eflags := strings.TrimSpace(e[1])

	h := strings.Split(e[0], ",")
	for i, v := range h {
		h[i] = strings.TrimSpace(v)
	}
	return h, eflags
}

// GetHeader ...
func GetHeader(fname string) ([]string, string) {
	f, err := os.Open(fname)
	if err != nil {
		log.Fatalf("读取表头定义失败: %s", err)
	}
	rd := bufio.NewReader(f)
	line, _, err := rd.ReadLine()
	if err != nil {
		log.Fatalf("读取表头定义失败: %s", err)
	}
	e := strings.Split(string(line), ":")
	if len(e) < 2 {
		log.Fatalln("表头定义中没有找到数据结束标记")
	}
	eflags := strings.TrimSpace(e[1])

	h := strings.Split(e[0], ",")
	for i, v := range h {
		h[i] = strings.TrimSpace(v)
	}
	return h, eflags
}

func deDumplicate(values []string) ([]string, map[string]int) {
	aggMap := make(map[string]int)
	for _, s := range values {
		v, ok := aggMap[s]
		if !ok {
			aggMap[s] = 1
		} else {
			aggMap[s] = v + 1
		}
	}

	data := make([]string, 0)
	for k, v := range aggMap {
		if v == 1 {
			delete(aggMap, k)
		}
		data = append(data, k)
	}
	return data, aggMap
}
