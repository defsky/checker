package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/tealeg/xlsx/v3"
)

// LoadFile ...
func LoadFile(fname string) *xlsx.File {
	log.Printf("加载数据文件: %s", fname)

	wb, err := xlsx.OpenFile(fname)
	if err != nil {
		log.Fatalln(err)
	}

	return wb
}

// var header=[]string{"定金（元）", "销售订单号", "来源采购订单", "专卖店", "订单时间", "品名", "规格", "终止原因"}

// DataRow ...
type DataRow struct {
	Money    float64 `json:"money"`
	SoDocNo  string  `json:"soDocNo"`
	SrcDocNo string  `json:"srcDocNo"`
	Customer string  `json:"customer"`
	DocTime  string  `json:"docTime"`
	ItemName string  `json:"itemName"`
	ItemSpec string  `json:"itemSpec"`
	Reason   string  `json:"reason"`
}

// SheetData ...
type SheetData struct {
	Header []string
	Data   [][]string
}

// GetColumnByName ...
func (sd *SheetData) GetColumnByName(name string) ([]string, error) {
	data := make([]string, 0)

	idx := -1
	for i, v := range sd.Header {
		if v == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, fmt.Errorf("没有找到指定的列：%s", name)
	}

	for _, v := range sd.Data {
		for j, vv := range v {
			if j == idx {
				data = append(data, vv)
				break
			}
		}
	}

	return data, nil
}

// LoadData ...
func LoadData(fname string, header []string, endflag string, needPrint bool) *SheetData {

	log.Printf("加载数据文件: %s", fname)

	wb, err := xlsx.OpenFile(fname)
	if err != nil {
		log.Fatalln(err)
	}

	data := &SheetData{
		Header: header,
		Data:   make([][]string, 0),
	}

DONE:
	for i, ws := range wb.Sheets {
		log.Printf("开始处理第 %d 个表单: %s, 行数：%d, 列数: %d", i+1, ws.Name, ws.MaxRow, ws.MaxCol)

		startCol, startRow := -1, -1
		status := WantHeader
		for j := 0; j < ws.MaxRow; j++ {
			row, err := ws.Row(j)
			if err != nil {
				log.Println(err)
				break
			}
			switch status {
			case WantHeader:
				if ok, start := MatchHeader(row, header); ok {
					startRow = j
					startCol = start
					status = WantData
					log.Printf("在第 %d 行匹配到表头，开始读取数据 ...", startRow+1)
					log.Printf("数据开始行号：%d，开始列号：%d\n", startRow+2, startCol+1)
				}
			case WantData:
				firstValue := GetCellData(row.GetCell(startCol))
				if strings.Contains(firstValue, endflag) {
					log.Printf("发现数据结束标记：%s", firstValue)
					status = FoundDataEnd
					break
				}

				rowdata := make([]string, 0)
				for k := startCol; k < startCol+len(header); k++ {
					cellValue := GetCellData(row.GetCell(k))
					rowdata = append(rowdata, cellValue)
				}
				data.Data = append(data.Data, rowdata)
				if needPrint {
					log.Printf("第 %d 行 ：%v", j-startRow, rowdata)
				}
			case FoundDataEnd:
			}
		}

		switch status {
		case WantHeader:
			log.Printf("表单 %s 没找到匹配的表头", ws.Name)
		case WantData:
			log.Printf("表单 %s 没有发现结束行", ws.Name)
		case FoundDataEnd:
			log.Printf("已从表单 %s 加载数据完毕，忽略其它表单", ws.Name)
			break DONE
		}

		ws.Close()
	}

	return data
}

// MatchHeader ...
func MatchHeader(row *xlsx.Row, header []string) (bool, int) {
	startCol := -1
	match, empty := true, true
	for k := 0; k < row.Sheet.MaxCol; k++ {
		cellValue := strings.TrimSpace(row.GetCell(k).String())
		//log.Printf("单元格值: %s", cellValue)

		if startCol == -1 {
			if len(cellValue) <= 0 {
				continue
			}
			empty = false
			startCol = k
		}

		headerIdx := k - startCol
		if headerIdx >= len(header) {
			break
		}
		if cellValue != header[headerIdx] {
			match = false
			break
		}
	}
	if empty {
		match = false
	}

	return match, startCol
}

// GetCellData ...
func GetCellData(c *xlsx.Cell) string {
	v := c.String()
	if c.IsTime() {
		t, err := c.GetTime(false)
		if err == nil {
			v = t.Format("2006-01-02 15:04:05")
		}
	}
	return strings.TrimSpace(v)
}

// GetSQLValueList ...
func GetSQLValueList(values []string) string {
	s := make([]string, 0)
	for _, v := range values {
		s = append(s, "'"+v+"'")
	}
	return strings.Join(s, ",")
}
