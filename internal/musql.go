package internal

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/antchfx/jsonquery"
	"github.com/antchfx/xmlquery"
	"github.com/antchfx/xpath"
	"github.com/frohmut/mustache"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type Musql struct {
	db *sql.DB
}

type FileInfo struct {
	Path      string
	Container string
}

type Select struct {
	Path string
	Name string
}

type FileContainer struct {
	container *zip.ReadCloser
	file      io.ReadCloser
}

func opencontainer(info FileInfo) (f *FileContainer, err error) {
	f = &FileContainer{}
	if info.Container == "" {
		f.file, err = os.Open(info.Path)
		if err != nil {
			return nil, err
		}
	} else {
		f.container, err = zip.OpenReader(info.Container)
		if err != nil {
			return nil, err
		}
		for _, fe := range f.container.File {
			if fe.Name == info.Path {
				f.file, err = fe.Open()
				if err != nil {
					return nil, err
				}
				break
			}
		}
		if f.file == nil {
			// file not found
			return nil, fmt.Errorf("could not find %s in %s", info.Path, info.Container)
		}
	}
	return f, nil
}

func (f *FileContainer) Close() {
	if f.file != nil {
		f.file.Close()
	}
	if f.container != nil {
		f.container.Close()
	}
}

func (m *Musql) NewDb() error {
	sqldb, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return err
	}
	m.db = sqldb
	return nil
}

func (m *Musql) OpenDb(filename string) error {
	sqldb, err := sql.Open("sqlite3", filename)
	if err != nil {
		return err
	}
	m.db = sqldb
	return nil
}

func (m *Musql) saveDb(filename string) {
}

func (m *Musql) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func recFlattenNode(prefix string, node xpath.NodeNavigator, header map[string]int, data map[string]string, usename string) error {
	if prefix != "" {
		prefix = prefix + "/"
	}

	nodea := node.Copy()
	for nodea.MoveToNextAttribute() {
		header[prefix+nodea.LocalName()] = len(header)
		data[prefix+nodea.LocalName()] = nodea.Value()
	}

	nodeData := node.LocalName()
	if usename != "" {
		nodeData = usename
	}
	child := node.Copy()
	for ok := child.MoveToChild(); ok; ok = child.MoveToNext() {
		if child.NodeType() == xpath.TextNode {
			if _, ok := header[nodeData]; ok {
				return fmt.Errorf("duplicate entry " + nodeData + " (" + prefix + ")")
			}
			header[nodeData] = len(header)
			data[nodeData] = child.Value()
		} else if child.NodeType() == xpath.ElementNode {
			nprefix := child.LocalName()
			if prefix != "" {
				nprefix = prefix + "/" + nprefix
			}
			err := recFlattenNode(prefix+child.LocalName(), child, header, data, "")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func mergemap(dest map[string]int, src map[string]int) {
	for k, _ := range src {
		if _, ok := dest[k]; !ok {
			dest[k] = len(dest)
		}
	}
}

func readTreeFile(path FileInfo, xpathstr string, xselects []Select, kind string) ([]string, []map[string]string, error) {
	f, err := opencontainer(path)
	if err != nil {
		err = fmt.Errorf("%w reading header of %s", err, path.Path)
		return nil, nil, err
	}
	defer f.Close()

	var docnode xpath.NodeNavigator
	if kind == "xml" {
		doc, err := xmlquery.Parse(f.file)
		if err != nil {
			return nil, nil, err
		}
		docnode = xmlquery.CreateXPathNavigator(doc)
	} else {
		doc, err := jsonquery.Parse(f.file)
		if err != nil {
			return nil, nil, err
		}
		docnode = jsonquery.CreateXPathNavigator(doc)
	}

	mheader := make(map[string]int)
	var data []map[string]string

	rexp, err := xpath.Compile(xpathstr)
	if err != nil {
		return nil, nil, err
	}
	rt := rexp.Select(docnode)
	for rt.MoveNext() {
		node := rt.Current()
		nheader := make(map[string]int)
		d := make(map[string]string)

		if len(xselects) == 0 {
			s := Select{Path: "."}
			xselects = append(xselects, s)
		}
		for _, xsel := range xselects {
			err_if_mis := true
			sel := xsel.Path
			if strings.HasSuffix(sel, "?") {
				sel = sel[:len(sel)-1]
				err_if_mis = false
			}
			exp, err := xpath.Compile(sel)
			if err != nil {
				return nil, nil, err
			}
			t := exp.Select(node.Copy())

			ok := t.MoveNext()
			if !ok {
				if err_if_mis {
					nodename := node.LocalName()
					if nodename == "" {
						nodename = "nameless node"
					}
					return nil, nil, fmt.Errorf("no element found for " + sel + " in " + nodename)
				}
				continue
			}

			curr := t.Current()
			nt := curr.NodeType()
			if nt == xpath.AttributeNode {
				h := curr.LocalName()
				if xsel.Name != "" {
					h = xsel.Name
				}
				nheader[h] = len(nheader)
				d[h] = curr.Value()
			} else {
				pref := ""
				err = recFlattenNode(pref, curr, nheader, d, xsel.Name)
				if err != nil {
					return nil, nil, err
				}
			}

			more_than_one := t.MoveNext()
			if more_than_one {
				return nil, nil, fmt.Errorf("more than one element found for " + sel + " in " + node.LocalName())
			}
		}

		mergemap(mheader, nheader)
		data = append(data, d)
	}

	var header []string
	for k, _ := range mheader {
		header = append(header, k)
	}
	sort.Strings(header)

	return header, data, nil
}

func addTreeFileToTable(db *sql.DB, info FileInfo, insert *sql.Stmt, header []string, xpathstr string, xselects []Select, kind string) error {
	var nheader []string
	var data []map[string]string
	var err error
	nheader, data, err = readTreeFile(info, xpathstr, xselects, kind)
	if err != nil {
		return err
	}
	err = verifyHeader(header, nheader)
	if err != nil {
		return err
	}
	for _, d := range data {
		var row []interface{}
		for _, h := range header {
			row = append(row, d[h])
		}
		_, err = insert.Exec(row...)
		if err != nil {
			return err
		}
	}
	return nil
}

func readHeaderCsv(info FileInfo, sep rune) ([]string, error) {
	f, err := opencontainer(info)
	if err != nil {
		err = fmt.Errorf("%w: reading header of %s", err, info.Path)
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f.file)
	r.Comma = sep
	header, err := r.Read()
	if err != nil {
		err = fmt.Errorf("%w: reading header of %s", err, info.Path)
		return nil, err
	}
	return header, nil
}

func deleteAllFromTable(db *sql.DB, tablename string) error {
	_, err := db.Exec(fmt.Sprintf("delete from \"%s\"", tablename))
	if err != nil {
		return fmt.Errorf("%w: deleting rows", err)
	}
	return nil
}

const (
	no_table = iota
	have_table
	bad_table
	had_err
)

// check if a table is already in the db and has the necessary columns
func haveTable(db *sql.DB, tablename string, header []string) (int, error) {
	rows, err := db.Query(fmt.Sprintf("select * from %s limit 1", tablename))
	if err != nil {
		return no_table, nil
	}
	columns, err := rows.Columns()
	if err != nil {
		return had_err, err
	}
	err = verifyHeader(columns, header)
	if err != nil {
		return bad_table, nil
	}
	for rows.Next() {
		// read rest
	}
	return have_table, nil
}

func verifyHeader(collist1 []string, collist2 []string) error {
	if len(collist1) != len(collist2) {
		return fmt.Errorf("header mismatch: >" + strings.Join(collist1, ";") + "< >" + strings.Join(collist2, ";") + "<")
	}
	for i, name := range collist1 {
		if name != collist2[i] {
			return fmt.Errorf("header mismatch: >" + strings.Join(collist1, ";") + "< >" + strings.Join(collist2, ";") + "<")
		}
	}
	return nil
}

func dropTable(db *sql.DB, tablename string) error {
	_, err := db.Exec(fmt.Sprintf("drop table \"%s\"", tablename))
	err = fmt.Errorf("%w: dropping table %s", err, tablename)
	return err
}

// ensure that the table exists in the database with the given columns
func ensureTable(db *sql.DB, tablename string, header []string) (err error) {
	if len(header) <= 0 {
		return fmt.Errorf("creating " + tablename + ": empty header")
	}
	// able already available? columns of table matching?
	info, err := haveTable(db, tablename, header)
	if err != nil {
		return err
	}
	if info == have_table {
		err := deleteAllFromTable(db, tablename)
		return err
	}
	if info == bad_table {
		err := dropTable(db, tablename)
		if err != nil {
			return err
		}
	}
	var ddl []string
	ddl = append(ddl, "create table")
	ddl = append(ddl, "\""+tablename+"\"")
	ddl = append(ddl, "(")
	for i, v := range header {
		if i > 0 {
			ddl = append(ddl, ",")
		}
		ddl = append(ddl, "\""+v+"\"")
	}
	ddl = append(ddl, ")")

	_, err = db.Exec(strings.Join(ddl, " "))
	if err != nil {
		return fmt.Errorf("%w: creating %s", err, tablename)
	}
	return nil
}

func addCsvFileToTable(db *sql.DB, info FileInfo, insert *sql.Stmt, header []string, sep rune, csvheader bool) error {
	f, err := opencontainer(info)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f.file)
	r.Comma = sep

	if csvheader {
		row, err := r.Read() // header
		if err != nil {
			return err
		}
		err = verifyHeader(header, row)
		if err != nil {
			return err
		}
	}

	for err == nil {
		var row []string
		row, err = r.Read()
		if err == nil {
			v := make([]interface{}, len(row))
			for i, val := range row {
				v[i] = val
			}
			_, err = insert.Exec(v...)
		}
	}
	if err == io.EOF {
		return nil
	}
	err = fmt.Errorf("%w: fill table from %s", err, info.Path)
	return err
}

func makeInsert(tx *sql.Tx, tablename string, header []string) (*sql.Stmt, error) {
	var query []string
	query = append(query, "insert into ")
	query = append(query, "\""+tablename+"\"")
	query = append(query, "values (")
	for i := 0; i < len(header); i++ {
		if i != 0 {
			query = append(query, ",")
		}
		query = append(query, "?")
	}
	query = append(query, ")")
	stmt, err := tx.Prepare(strings.Join(query, " "))
	if err != nil {
		return nil, fmt.Errorf("%w preparing insert", err)
	}
	return stmt, nil
}

func (m *Musql) AddDatabase(fname string, name string) error {
	_, err := m.db.Exec("attach '" + fname + "' as " + name)
	return err
}

func (m *Musql) ApplySql(fname string) error {

	// run
	b, err := os.ReadFile(fname)
	if err != nil {
		return err
	}
	_, err = m.db.Exec(string(b))
	if err != nil {
		return err
	}
	return nil
}

func (m *Musql) TablesToContext(mdata map[string]interface{}) error {
	// collect new/updated views/tables
	objects, err := m.db.Query("select tbl_name from sqlite_master")
	if err != nil {
		return err
	}
	var objlist []string
	for objects.Next() {
		var objname string
		err := objects.Scan(&objname)
		if err != nil {
			return err
		}
		if objname[0] == '_' {
			// ignore "temporary" tables/views
			continue
		}
		objlist = append(objlist, objname)
	}

	for _, obj := range objlist {
		err := m.addData(mdata, obj, fmt.Sprintf("select * from %s", obj))
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Musql) addData(mdata map[string]interface{}, resultvar string, stmt string) error {
	rows, err := m.db.Query(stmt)
	if err != nil {
		return err
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	var res []map[string]interface{}

	for rows.Next() {
		cols := len(columns)
		valary := make([]interface{}, cols)
		valptr := make([]interface{}, cols)
		values := make(map[string]*interface{})
		for i, name := range columns {
			valptr[i] = &valary[i]
			values[name] = &valary[i]
		}
		err := rows.Scan(valptr...)
		if err != nil {
			return err
		}
		xvalues := make(map[string]interface{})
		for name, v := range values {
			s, ok := (*v).([]uint8)
			if ok {
				xvalues[name] = string(s)
			} else {
				xvalues[name] = *v
			}
		}
		res = append(res, xvalues)
	}
	mdata[resultvar] = res

	have_rows := false
	if len(res) > 0 {
		have_rows = true
	}
	mdata[resultvar+"?"] = have_rows
	return nil
}

func (m *Musql) AddCsv(tablename string, path []FileInfo, sep rune) error {
	err := m.addCsvFiles(tablename, path, sep, nil)
	return err
}

func (m *Musql) AddCsvWithHeader(tablename string, path []FileInfo, sep rune, header []string) error {
	err := m.addCsvFiles(tablename, path, sep, header)
	return err
}

func (m *Musql) addCsvFiles(tablename string, path []FileInfo, sep rune, inheader []string) error {
	// read first csv -> header -> columns
	if sep == 0 {
		sep = ';'
	}
	var header []string
	var insert *sql.Stmt
	var tx *sql.Tx
	var csvheader = true
	var err error

	if inheader != nil {
		header = inheader
		csvheader = false
		err = ensureTable(m.db, tablename, header)
		if err != nil {
			return err
		}
		tx, err = m.db.Begin()
		if err != nil {
			return err
		}
		defer tx.Commit()
		insert, err = makeInsert(tx, tablename, header)
		if err != nil {
			return err
		}
	}
	// add data from all files
	for _, fileinfo := range path {
		patt := fileinfo.Path
		if fileinfo.Container != "" {
			patt = fileinfo.Container
		}
		flist, err := filepath.Glob(patt)
		if err != nil {
			return err
		}
		if len(flist) == 0 {
			return fmt.Errorf("file " + patt + " not found")
		}
		for _, fname := range flist {
			f := FileInfo{Path: fname, Container: fileinfo.Container}
			if len(header) == 0 {
				header, err = readHeaderCsv(f, sep)
				if err != nil {
					return err
				}
				err = ensureTable(m.db, tablename, header)
				if err != nil {
					return err
				}
				tx, err = m.db.Begin()
				if err != nil {
					return err
				}
				defer tx.Commit()
				insert, err = makeInsert(tx, tablename, header)
				if err != nil {
					return err
				}
			}
			err = addCsvFileToTable(m.db, f, insert, header, sep, csvheader)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Musql) AddFromTreeFile(tablename string, path []FileInfo, xpathstr string, xselects []Select, kind string) error {
	var header []string
	var err error
	header, _, err = readTreeFile(path[0], xpathstr, xselects, kind)
	if err != nil {
		return err
	}
	err = ensureTable(m.db, tablename, header)
	if err != nil {
		return err
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Commit()

	insert, err := makeInsert(tx, tablename, header)
	if err != nil {
		return err
	}
	// add data from all files
	for _, filename := range path {
		err = addTreeFileToTable(m.db, filename, insert, header, xpathstr, xselects, kind)
		if err != nil {
			break
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (m *Musql) AddXml(tablename string, path []FileInfo, xpathstr string, xselects []Select) error {
	err := m.AddFromTreeFile(tablename, path, xpathstr, xselects, "xml")
	return err
}

func (m *Musql) AddJson(tablename string, path []FileInfo, xpathstr string, xselects []Select) error {
	err := m.AddFromTreeFile(tablename, path, xpathstr, xselects, "json")
	return err
}

func (m *Musql) AddFiles(tablename string, root string, withcontent bool) error {
	header := []string{"fullpath", "filename", "content"}
	err := ensureTable(m.db, tablename, header)
	if err != nil {
		return err
	}

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Commit()

	insert, err := makeInsert(tx, tablename, header)
	if err != nil {
		return err
	}
	err = filepath.Walk(root, func(fullpath string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		content := ""
		if withcontent {
			b, err := ioutil.ReadFile(fullpath)
			if err != nil {
				return err
			}
			content = string(b)
		}
		fn := path.Base(fullpath)
		_, err = insert.Exec(fullpath, fn, content)
		if err != nil {
			return fmt.Errorf("%w: storing file info for %s", err, fn)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *Musql) AddParameters(tablename string, params map[string]string) error {
	header := []string{"paramkey", "value"}
	err := ensureTable(m.db, tablename, header)
	if err != nil {
		return err
	}

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Commit()

	insert, err := makeInsert(tx, tablename, header)
	for key, value := range params {
		_, err = insert.Exec(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Musql) RunTemplateFile(filename string, out io.Writer) error {
	templatestring, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	err = m.RunTemplate(string(templatestring), out)
	if err != nil {
		return fmt.Errorf("%w for %s", err, filename)
	}
	return nil
}

func (m *Musql) runTemplateWithData(templatestring string, out io.Writer, mdata map[string]interface{}) error {
	mdata["error"] = func(rawtxt string, render mustache.RenderFn) (string, error) {
		var err error
		var empty string
		txt, err := render(rawtxt)
		if err != nil {
			return empty, err
		}
		err = fmt.Errorf(txt)
		return empty, err
	}
	mdata["sql"] = func(rawstmt string, render mustache.RenderFn) (string, error) {
		// bug in sqlite3-go? rows.Next() never returns false for empty statement string
		stmt, err := render(rawstmt)
		if err != nil {
			return "", err
		}
		if stmt == "" {
			return "", nil
		}
		db := m.db
		resultvar := "result"
		re := regexp.MustCompile("create *(view|table) *([^ ]*) as")
		sm := re.FindStringSubmatch(stmt)
		i := regexp.MustCompile("insert into")
		im := i.FindStringSubmatch(stmt)
		ve := regexp.MustCompile("create *var *([^ ]*) *as *((?s).*)$")
		vm := ve.FindStringSubmatch(stmt)
		we := regexp.MustCompile("with *fragment *([^ ]*) as[ \r\n]*((?s).*)[\r\n]+$")
		wm := we.FindStringSubmatch(stmt)
		if sm != nil {
			// before creating a view, drop existing ones
			_, err := db.Exec(fmt.Sprintf("drop %s if exists \"%s\"", sm[1], sm[2]))
			if err != nil {
				return "", err
			}
			// creating a table/view always selects
			// afterwards and writes the result to a variable
			// named after the view
			resultvar = "" + sm[2]
			_, err = db.Exec(stmt)
			if err != nil {
				return "", err
			}
			stmt = "select * from " + resultvar
		} else if vm != nil {
			resultvar = vm[1]
			stmt = vm[2]
		} else if im != nil {
			// 'pure statement': not a query
			_, err := db.Exec(stmt)
			return "", err
		} else if wm != nil {
			// store text
			mdata[wm[1]] = wm[2]
			return "", nil
		}

		err = m.addData(mdata, resultvar, stmt)
		if err != nil {
			return "", err
		}

		return "", nil
	}

	err := m.TablesToContext(mdata)
	if err != nil {
		return err
	}

	mustache.AllowMissingVariables = false
	mtempl, err := mustache.ParseString(string(templatestring))
	if err != nil {
		return fmt.Errorf("%w (parsing the mustache template)", err)
	}
	err = mtempl.FRender(out, mdata)
	if err != nil {
		return fmt.Errorf("%w (executing the mustache template)", err)
	}
	return nil
}

func (m *Musql) RunTemplate(templatestring string, out io.Writer) error {
	mdata := map[string]interface{}{}
	return m.runTemplateWithData(templatestring, out, mdata)
}
