// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// dm operators.
var dmOperators = map[string]string{
	"exact":       "= ?",
	"gt":          "> ?",
	"gte":         ">= ?",
	"lt":          "< ?",
	"lte":         "<= ?",
	"//iendswith": "LIKE ?",
}

// dm column field types.
var dmTypes = map[string]string{
	"pk":              "NOT NULL PRIMARY KEY",
	"bool":            "TINYINT",
	"string":          "VARCHAR2(%d)",
	"string-char":     "CHAR(%d)",
	"string-text":     "VARCHAR2(%d)",
	"time.Time-date":  "DATE",
	"time.Time":       "TIMESTAMP",
	"int8":            "INTEGER",
	"int16":           "INTEGER",
	"int32":           "INTEGER",
	"int64":           "INTEGER",
	"uint8":           "INTEGER",
	"uint16":          "INTEGER",
	"uint32":          "INTEGER",
	"uint64":          "INTEGER",
	"float64":         "NUMBER",
	"float64-decimal": "NUMBER(%d, %d)",
}

// songwei dm Reserved keywords  预留关键字适配
var dmKeywordsTolowTypes = map[string]bool{
	`"ORDER"`:    true,
	`"USAGE"`:    true,
	`"DOMAIN"`:   true,
	`"INTERVAL"`: true,
	`"STORAGE"`:  true,
	`"TEMPLATE"`: true,
	`"SUCCESS"`:  true,
	`"TYPE"`:     true,
	`"ROLE"`:     true,
	`"AUTHID"`:   true,
	`"FORMAT"`:   true,
	`"MODE"`:     true,
	`"NODE"`:     true,
}

// songwei dm Reserved keywords  预留关键字适配
var dmKeywordsToUpperTypes = map[string]bool{
	"order":    true,
	"usage":    true,
	"domain":   true,
	"interval": true,
	"storage":  true,
	"template": true,
	"success":  true,
	"type":     true,
	"role":     true,
	"authid":   true,
	"fromat":   true,
	"mode":     true,
	"node":     true,
}

// dm dbBaser
type dbBasedm struct {
	dbBase
}

var _ dbBaser = new(dbBasedm)

// create dm dbBaser.
func newdbBaseDm() dbBaser {
	b := new(dbBasedm)
	b.ins = b
	return b
}

// return quote.
func (d *dbBasedm) TableQuote() string {
	// songwei 适配DM 去掉return "`" 直接返回空
	return ""

}

//songwei DM适配 关键字
// get struct columns values as interface slice.

func (d *dbBasedm) collectValues(mi *modelInfo, ind reflect.Value, cols []string, skipAuto bool, insert bool, names *[]string, tz *time.Location) (values []interface{}, autoFields []string, err error) {
	if names == nil {
		ns := make([]string, 0, len(cols))
		names = &ns
	}
	values = make([]interface{}, 0, len(cols))

	for _, column := range cols {
		var fi *fieldInfo
		ll := strings.ToLower(strings.Replace(column, `"`, ``, -1))
		if fi, _ = mi.fields.GetByAny(ll); fi != nil {
			column = fi.column
		} else {
			panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", column, mi.fullName))
		}
		if !fi.dbcol || fi.auto && skipAuto {
			continue
		}
		value, err := d.collectFieldValue(mi, fi, ind, insert, tz)
		if err != nil {
			return nil, nil, err
		}

		// ignore empty value auto field
		if insert && fi.auto {
			if fi.fieldType&IsPositiveIntegerField > 0 {
				if vu, ok := value.(uint64); !ok || vu == 0 {
					continue
				}
			} else {
				if vu, ok := value.(int64); !ok || vu == 0 {
					continue
				}
			}
			autoFields = append(autoFields, fi.column)
		}

		//songwei 适配DM 关键字添加
		if dmKeywordsToUpperTypes[column] {
			column = `"` + strings.ToUpper(column) + `"`
		}

		*names, values = append(*names, column), append(values, value)
	}

	return
}

//songwei 从写db.go 中的setColsValues 方法适配DM 关键字
func (d *dbBasedm) setColsValues(mi *modelInfo, ind *reflect.Value, cols []string, values []interface{}, tz *time.Location) {
	for i, column := range cols {
		val := reflect.Indirect(reflect.ValueOf(values[i])).Interface()

		//songwei 处理DM预留关键字

		if dmKeywordsTolowTypes[column] {
			column = strings.ToLower(strings.Replace(column, `"`, ``, -1))
		}

		fi := mi.fields.GetByColumn(column)

		field := ind.FieldByIndex(fi.fieldIndex)

		value, err := d.convertValueFromDB(fi, val, tz)
		if err != nil {
			panic(fmt.Errorf("Raw value: `%v` %s", val, err.Error()))
		}

		_, err = d.setFieldValue(fi, value, field)

		if err != nil {
			panic(fmt.Errorf("Raw value: `%v` %s", val, err.Error()))
		}
	}
}

func (d *dbBasedm) Read(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string, isForUpdate bool) error {
	var whereCols []string
	var args []interface{}

	// if specify cols length > 0, then use it for where condition.
	if len(cols) > 0 {
		var err error
		whereCols = make([]string, 0, len(cols))
		args, _, err = d.collectValues(mi, ind, cols, false, false, &whereCols, tz)
		if err != nil {
			return err
		}
	} else {
		// default use pk value as where condtion.
		pkColumn, pkValue, ok := getExistPk(mi, ind)
		if !ok {
			return ErrMissPK
		}
		whereCols = []string{pkColumn}
		args = append(args, pkValue)
	}

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s, %s", Q, Q)
	//sels := strings.Join(mi.fields.dbcols, sep)
	tCols := mi.fields.dbcols
	for i, col := range tCols {
		if dmKeywordsToUpperTypes[col] {
			tCols[i] = `"` + strings.ToUpper(col) + `"`
		}
	}
	sels := fmt.Sprintf("%s%s%s", Q, strings.Join(tCols, sep), Q)
	colsNum := len(mi.fields.dbcols)

	sep = fmt.Sprintf("%s = ? AND %s", Q, Q)
	wheres := strings.Join(whereCols, sep)

	forUpdate := ""
	if isForUpdate {
		forUpdate = "FOR UPDATE"
	}

	query := fmt.Sprintf("SELECT %s%s%s FROM %s%s%s WHERE %s%s%s = ? %s", Q, sels, Q, Q, mi.table, Q, Q, wheres, Q, forUpdate)

	refs := make([]interface{}, colsNum)
	for i := range refs {
		var ref interface{}
		refs[i] = &ref
	}

	d.ins.ReplaceMarks(&query)

	row := q.QueryRow(query, args...)
	if err := row.Scan(refs...); err != nil {
		if err == sql.ErrNoRows {
			return ErrNoRows
		}
		return err
	}
	elm := reflect.New(mi.addrField.Elem().Type())
	mind := reflect.Indirect(elm)
	d.setColsValues(mi, &mind, mi.fields.dbcols, refs, tz)
	ind.Set(mind)
	return nil
}

// 从写db.go ReadBatch 方法适配DM关键字
func (d *dbBasedm) ReadBatch(q dbQuerier, qs *querySet, mi *modelInfo, cond *Condition, container interface{}, tz *time.Location, cols []string) (int64, error) {

	val := reflect.ValueOf(container)
	ind := reflect.Indirect(val)

	errTyp := true
	one := true
	isPtr := true

	if val.Kind() == reflect.Ptr {
		fn := ""
		if ind.Kind() == reflect.Slice {
			one = false
			typ := ind.Type().Elem()
			switch typ.Kind() {
			case reflect.Ptr:
				fn = getFullName(typ.Elem())
			case reflect.Struct:
				isPtr = false
				fn = getFullName(typ)
			}
		} else {
			fn = getFullName(ind.Type())
		}
		errTyp = fn != mi.fullName
	}

	if errTyp {
		if one {
			panic(fmt.Errorf("wrong object type `%s` for rows scan, need *%s", val.Type(), mi.fullName))
		} else {
			panic(fmt.Errorf("wrong object type `%s` for rows scan, need *[]*%s or *[]%s", val.Type(), mi.fullName, mi.fullName))
		}
	}

	rlimit := qs.limit
	offset := qs.offset

	Q := d.ins.TableQuote()

	var tCols []string
	if len(cols) > 0 {
		hasRel := len(qs.related) > 0 || qs.relDepth > 0
		tCols = make([]string, 0, len(cols))
		var maps map[string]bool
		if hasRel {
			maps = make(map[string]bool)
		}
		for _, col := range cols {
			if fi, ok := mi.fields.GetByAny(col); ok {
				tCols = append(tCols, fi.column)
				if hasRel {
					maps[fi.column] = true
				}
			} else {
				return 0, fmt.Errorf("wrong field/column name `%s`", col)
			}
		}
		if hasRel {
			for _, fi := range mi.fields.fieldsDB {
				if fi.fieldType&IsRelField > 0 {
					if !maps[fi.column] {
						tCols = append(tCols, fi.column)
					}
				}
			}
		}
	} else {
		tCols = mi.fields.dbcols
	}
	//songwei 处理 DM 关键字
	for i, col := range tCols {
		if dmKeywordsToUpperTypes[col] {
			tCols[i] = `"` + strings.ToUpper(col) + `"`
		}
	}

	colsNum := len(tCols)
	sep := fmt.Sprintf("%s, T0.%s", Q, Q)
	sels := fmt.Sprintf("T0.%s%s%s", Q, strings.Join(tCols, sep), Q)

	tables := newDbTables(mi, d.ins)
	tables.parseRelated(qs.related, qs.relDepth)

	where, args := tables.getCondSQL(cond, false, tz)
	groupBy := tables.getGroupSQL(qs.groups)
	orderBy := tables.getOrderSQL(qs.orders)
	limit := tables.getLimitSQL(mi, offset, rlimit)
	join := tables.getJoinSQL()

	for _, tbl := range tables.tables {
		if tbl.sel {
			colsNum += len(tbl.mi.fields.dbcols)
			sep := fmt.Sprintf("%s, %s.%s", Q, tbl.index, Q)
			sels += fmt.Sprintf(", %s.%s%s%s", tbl.index, Q, strings.Join(tbl.mi.fields.dbcols, sep), Q)
		}
	}

	sqlSelect := "SELECT"
	if qs.distinct {
		sqlSelect += " DISTINCT"
	}
	query := fmt.Sprintf("%s %s FROM %s%s%s T0 %s%s%s%s%s", sqlSelect, sels, Q, mi.table, Q, join, where, groupBy, orderBy, limit)

	if qs.forupdate {
		query += " FOR UPDATE"
	}

	d.ins.ReplaceMarks(&query)

	var rs *sql.Rows
	var err error
	if qs != nil && qs.forContext {
		rs, err = q.QueryContext(qs.ctx, query, args...)
		if err != nil {
			return 0, err
		}
	} else {
		rs, err = q.Query(query, args...)
		if err != nil {
			return 0, err
		}
	}

	refs := make([]interface{}, colsNum)
	for i := range refs {
		var ref interface{}
		refs[i] = &ref
	}

	defer rs.Close()

	slice := ind

	var cnt int64
	for rs.Next() {
		if one && cnt == 0 || !one {
			if err := rs.Scan(refs...); err != nil {
				return 0, err
			}

			elm := reflect.New(mi.addrField.Elem().Type())
			mind := reflect.Indirect(elm)

			cacheV := make(map[string]*reflect.Value)
			cacheM := make(map[string]*modelInfo)
			trefs := refs

			d.setColsValues(mi, &mind, tCols, refs[:len(tCols)], tz)
			trefs = refs[len(tCols):]

			for _, tbl := range tables.tables {
				// loop selected tables
				if tbl.sel {
					last := mind
					names := ""
					mmi := mi
					// loop cascade models
					for _, name := range tbl.names {
						names += name
						if val, ok := cacheV[names]; ok {
							last = *val
							mmi = cacheM[names]
						} else {
							fi := mmi.fields.GetByName(name)
							lastm := mmi
							mmi = fi.relModelInfo
							field := last
							if last.Kind() != reflect.Invalid {
								field = reflect.Indirect(last.FieldByIndex(fi.fieldIndex))
								if field.IsValid() {
									d.setColsValues(mmi, &field, mmi.fields.dbcols, trefs[:len(mmi.fields.dbcols)], tz)
									for _, fi := range mmi.fields.fieldsReverse {
										if fi.inModel && fi.reverseFieldInfo.mi == lastm {
											if fi.reverseFieldInfo != nil {
												f := field.FieldByIndex(fi.fieldIndex)
												if f.Kind() == reflect.Ptr {
													f.Set(last.Addr())
												}
											}
										}
									}
									last = field
								}
							}
							cacheV[names] = &field
							cacheM[names] = mmi
						}
					}
					trefs = trefs[len(mmi.fields.dbcols):]
				}
			}

			if one {
				ind.Set(mind)
			} else {
				if cnt == 0 {
					// you can use a empty & caped container list
					// orm will not replace it
					if ind.Len() != 0 {
						// if container is not empty
						// create a new one
						slice = reflect.New(ind.Type()).Elem()
					}
				}

				if isPtr {
					slice = reflect.Append(slice, mind.Addr())
				} else {
					slice = reflect.Append(slice, mind)
				}
			}
		}
		cnt++
	}

	if !one {
		if cnt > 0 {
			ind.Set(slice)
		} else {
			// when a result is empty and container is nil
			// to set a empty container
			if ind.IsNil() {
				ind.Set(reflect.MakeSlice(ind.Type(), 0, 0))
			}
		}
	}

	return cnt, nil
}

// OperatorSQL get dm operator.
func (d *dbBasedm) OperatorSQL(operator string) string {
	return dmOperators[operator]
}

// DbTypes get dm table field types.
func (d *dbBasedm) DbTypes() map[string]string {
	return dmTypes
}

//ShowTablesQuery show all the tables in database
func (d *dbBasedm) ShowTablesQuery() string {
	return "SELECT TABLE_NAME FROM USER_TABLES"
}

// dm
func (d *dbBasedm) ShowColumnsQuery(table string) string {
	return fmt.Sprintf("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS "+
		"WHERE TABLE_NAME ='%s'", strings.ToUpper(table))
}

// check index is exist
func (d *dbBasedm) IndexExists(db dbQuerier, table string, name string) bool {
	row := db.QueryRow("SELECT COUNT(*) FROM USER_IND_COLUMNS, USER_INDEXES "+
		"WHERE USER_IND_COLUMNS.INDEX_NAME = USER_INDEXES.INDEX_NAME "+
		"AND  USER_IND_COLUMNS.TABLE_NAME = ? AND USER_IND_COLUMNS.INDEX_NAME = ?", strings.ToUpper(table), strings.ToUpper(name))

	var cnt int
	row.Scan(&cnt)
	return cnt > 0
}

// songwei 适配DM 插入 关键字
// execute insert sql dbQuerier with given struct reflect.Value.
func (d *dbBasedm) Insert(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location) (int64, error) {
	names := make([]string, 0, len(mi.fields.dbcols))
	values, autoFields, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
	if err != nil {
		return 0, err
	}

	id, err := d.InsertValue(q, mi, false, names, values)
	if err != nil {
		return 0, err
	}

	if len(autoFields) > 0 {
		err = d.ins.setval(q, mi, autoFields)
	}
	return id, err
}

//songwei 适配DM更新
// execute update sql dbQuerier with given struct reflect.Value.

func (d *dbBasedm) Update(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (int64, error) {
	pkName, pkValue, ok := getExistPk(mi, ind)
	if !ok {
		return 0, ErrMissPK
	}

	var setNames []string

	// if specify cols length is zero, then commit all columns.
	if len(cols) == 0 {
		cols = mi.fields.dbcols
		setNames = make([]string, 0, len(mi.fields.dbcols)-1)
	} else {
		setNames = make([]string, 0, len(cols))
	}

	setValues, _, err := d.collectValues(mi, ind, cols, true, false, &setNames, tz)
	if err != nil {
		return 0, err
	}

	var findAutoNowAdd, findAutoNow bool
	var index int
	for i, col := range setNames {
		//songwei DM适配关键字
		if dmKeywordsTolowTypes[col] {
			col = strings.ToLower(strings.Replace(col, `"`, ``, -1))
		}
		if mi.fields.GetByColumn(col).autoNowAdd {
			index = i
			findAutoNowAdd = true
		}
		if mi.fields.GetByColumn(col).autoNow {
			findAutoNow = true
		}

	}
	if findAutoNowAdd {
		setNames = append(setNames[0:index], setNames[index+1:]...)
		setValues = append(setValues[0:index], setValues[index+1:]...)
	}

	if !findAutoNow {
		for col, info := range mi.fields.columns {
			if info.autoNow {
				setNames = append(setNames, col)
				setValues = append(setValues, time.Now())
			}
		}
	}

	setValues = append(setValues, pkValue)

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s = ?, %s", Q, Q)
	setColumns := strings.Join(setNames, sep)

	query := fmt.Sprintf("UPDATE %s%s%s SET %s%s%s = ? WHERE %s%s%s = ?", Q, mi.table, Q, Q, setColumns, Q, Q, pkName, Q)

	d.ins.ReplaceMarks(&query)

	res, err := q.Exec(query, setValues...)
	if err == nil {
		return res.RowsAffected()
	}
	return 0, err
}

// execute insert sql with given struct and given values.
// insert the given values, not the field values in struct.
/*
func (d *dbBasedm) InsertValue(q dbQuerier, mi *modelInfo, isMulti bool, names []string, values []interface{}) (int64, error) {
	Q := d.ins.TableQuote()

	marks := make([]string, len(names))
	for i := range marks {
		marks[i] = ":" + names[i]
	}

	for i := range names {
		//songwei 适配DM 关键字添加
		if dmKeywordsToUpperTypes[names[i]] {
			names[i] = `"` + strings.ToUpper(names[i]) + `"`
		}
	}

	sep := fmt.Sprintf("%s, %s", Q, Q)
	qmarks := strings.Join(marks, ", ")
	columns := strings.Join(names, sep)

	multi := len(values) / len(names)

	if isMulti {
		qmarks = strings.Repeat(qmarks+"), (", multi-1) + qmarks
	}

	query := fmt.Sprintf("INSERT INTO %s%s%s (%s%s%s) VALUES (%s)", Q, mi.table, Q, Q, columns, Q, qmarks)

	d.ins.ReplaceMarks(&query)

	if isMulti || !d.ins.HasReturningID(mi, &query) {
		res, err := q.Exec(query, values...)
		if err == nil {
			if isMulti {
				return res.RowsAffected()
			}
			return res.LastInsertId()
		}
		return 0, err
	}
	row := q.QueryRow(query, values...)
	var id int64
	err := row.Scan(&id)
	return id, err
}
*/
