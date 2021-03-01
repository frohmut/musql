package internal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

type tabinfo struct {
	Tablename string
	Files     []FileInfo
	Type      string
	// csv
	Sep    rune
	Header []string
	// files
	Content bool
	// xml
	XPath   string
	XSelect []Select
}

type templinfo struct {
	TemplateName   string
	TemplateString string
	Outname        string
}

func getPath(basedir string, fname string) string {
	return path.Join(basedir, fname)
}

func ArgSource(argv []string, start int, basedir string, tables *[]*tabinfo) (int, error) {
	// insert {<filename>} into <name> [as <type>]
	t := &tabinfo{}
	i := start
	if start >= len(argv) || (argv[start] != "insert" && argv[start] != "-insert") {
		return start, nil
	}
	i++
	for i < len(argv) && argv[i] != "into" {
		f := FileInfo{}
		f.Path = getPath(basedir, argv[i])
		i++
		if i < len(argv) && argv[i] == "from" {
			i++
			if i >= len(argv) {
				return start, fmt.Errorf("missing container file after 'from'")
			}
			f.Container = getPath(basedir, argv[i])
			i++
		}
		t.Files = append(t.Files, f)
	}
	if i >= len(argv) {
		return start, fmt.Errorf("Missing 'into' for insert")
	}
	i++
	t.Tablename = argv[i]
	i++
	if i < len(argv) && argv[i] == "as" {
		i++
		if i >= len(argv) {
			return start, fmt.Errorf("Missing file-type after 'as'")
		}
		t.Type = argv[i]
		i++
	}

	if i < len(argv) && argv[i] == "separator" {
		i++
		if i >= len(argv) {
			return start, fmt.Errorf("Missing separator after 'separator'")
		}
		t.Sep, _ = utf8.DecodeRuneInString(argv[i])
		i++
	}
	if i < len(argv) && argv[i] == "with" {
		i++
		if i < len(argv) && argv[i] == "content" {
			i++
			t.Content = true
		} else {
			for i < len(argv) && argv[i] != "as" {
				t.Header = append(t.Header, argv[i])
				i++
			}
			if i+1 >= len(argv)+1 || argv[i] != "as" || argv[i+1] != "header" {
				return start, fmt.Errorf("Missing 'content' or 'as header' after 'with'")
			}
			i++
			i++
		}
	} else {
		if i < len(argv) && argv[i] == "using" {
			i++
			for i < len(argv) && argv[i] != "from" {
				s := Select{}
				s.Path = argv[i]
				i++
				if i < len(argv) && argv[i] == "as" {
					i++
					if i >= len(argv) {
						return start, fmt.Errorf("Missing name after 'as'")
					}
					s.Name = argv[i]
					i++
				}
				t.XSelect = append(t.XSelect, s)
			}
			if i >= len(argv)-1 || argv[i] != "from" || argv[i+1] != "xpath" {
				return start, fmt.Errorf("Missing 'from xpath' after 'using' list")
			}
			i++
		}
		if i < len(argv) && argv[i] == "xpath" {
			i++
			if i >= len(argv) {
				return start, fmt.Errorf("Missing xpath after 'xpath'")
			}
			t.XPath = argv[i]
			i++
		}
	}

	*tables = append(*tables, t)
	return i, nil
}

func ArgTemplate(argv []string, start int, basedir string, templates *[]*templinfo) (int, error) {
	i := start
	if i >= len(argv) || (argv[i] != "expand" && argv[i] != "-expand") {
		return i, nil
	}
	i++
	if i >= len(argv) {
		return i, fmt.Errorf("Missing template name after 'run'")
	}
	t := &templinfo{}
	t.TemplateName = getPath(basedir, argv[i])
	i++
	if i < len(argv) && argv[i] == "as" {
		i++
		if i >= len(argv) {
			return start, fmt.Errorf("Missing output name after 'as'")
		}
		t.Outname = getPath(basedir, argv[i])
		i++
	} else {
		t.Outname = "stdout"
	}

	tempstring, err := ioutil.ReadFile(t.TemplateName)
	if err != nil {
		return start, err
	}
	t.TemplateString = string(tempstring)

	*templates = append(*templates, t)
	return i, nil
}

func ArgSelect(argv []string, start int, _ string, templates *[]*templinfo) (int, error) {
	i := start
	if i >= len(argv) || (argv[i] != "select" && argv[i] != "-select") {
		return i, nil
	}
	var txt []string
	for ; i < len(argv) && argv[i] != "expanding"; i++ {
		txt = append(txt, argv[i])
	}
	if i >= len(argv) {
		return start, fmt.Errorf("Missing 'expanding' for select")
	}
	i++
	expand := argv[i]
	i++
	t := &templinfo{}
	t.TemplateName = "command line"
	t.Outname = "stdout"
	t.TemplateString = fmt.Sprintf("{{#sql}}%s{{/sql}}%s", strings.Join(txt, " "), expand)
	*templates = append(*templates, t)
	return i, nil
}

func ArgIni(argv []string, start int, basedir string, args *arglist) (int, error) {
	i := start
	if i >= len(argv) || (argv[i] != "ini" && argv[i] != "-ini" && argv[i] != "-defini") {
		return i, nil
	}
	i++
	ininame := getPath(basedir, argv[i])
	i++
	dat, err := ioutil.ReadFile(ininame)
	if err != nil {
		if argv[start] == "-defini" {
			return i, nil
		}
		return i, err
	}
	s := string(dat)
	words := regexp.MustCompile("[ \n]+").Split(s, -1)
	var nap = argpart{}
	nap.basedir = filepath.Dir(ininame)
	nap.argv = words
	args.parts = append(args.parts, nap)
	return i, nil
}

func ArgIgnoreEmpty(argv []string, i int, _ string) (int, error) {
	if i < len(argv) && argv[i] == "" {
		i++
	}
	return i, nil
}

func ArgIgnoreComment(argv []string, start int, _ string) (int, error) {
	i := start
	if i >= len(argv) || argv[i] != "/*" {
		return start, nil
	}
	i++
	for ; i < len(argv) && argv[i] != "*/"; i++ {
		// ignore comment
	}
	if i >= len(argv) {
		return i, fmt.Errorf("Missing closing '*/' for comment '" + strings.Join(argv[start:], " ") + "'")
	}
	i++
	return i, nil
}

func ArgDB(argv []string, i int, basedir string, s *string) (int, error) {
	if i < len(argv)-1 && argv[i] == "db" {
		i++
		*s = getPath(basedir, argv[i])
		i++
	}
	return i, nil
}

func ArgAttach(argv []string, i int, basedir string, a map[string]string) (int, error) {
	if i >= len(argv) || argv[i] != "attach" {
		return i, nil
	}
	i++
	if i >= len(argv) {
		return i, fmt.Errorf("Missing filename of sqlite db")
	}
	fname := getPath(basedir, argv[i])
	i++
	if i >= len(argv) || argv[i] != "as" {
		return i, fmt.Errorf("Missing 'as' for sqlite db")
	}
	i++
	if i >= len(argv) {
		return i, fmt.Errorf("Missing name for attached sqlite db")
	}
	name := argv[i]
	i++
	a[fname] = name
	return i, nil
}

func ArgParam(argv []string, i int, _ string, p map[string]string) (int, error) {
	if i < len(argv)-2 && argv[i] == "set" {
		i++
		key := argv[i]
		i++
		val := argv[i]
		i++
		p[key] = val
	}
	return i, nil
}

func ArgSql(argv []string, i int, basedir string, p *[]string) (int, error) {
	if i >= len(argv) || argv[i] != "sql" {
		return i, nil
	}
	i++
	if i >= len(argv) {
		return i, fmt.Errorf("Missing sql filename")
	}
	fname := getPath(basedir, argv[i])
	i++
	*p = append(*p, fname)
	return i, nil
}

type Parser func(argv []string, i int, basedir string) (int, error)

type argpart struct {
	basedir string
	argv    []string
}

type arglist struct {
	parts []argpart
}

type Config struct {
	tabinfos     []*tabinfo
	templates    []*templinfo
	sqls         []string
	dbname       string
	params       map[string]string
	dbs          map[string]string
	parsers      []Parser
	parsersready bool
	allargs      arglist
}

func (c *Config) AddParser(p Parser) {
	c.parsers = append(c.parsers, p)
}

func (c *Config) Init() {
	c.params = make(map[string]string)
	c.dbs = make(map[string]string)

	c.AddParser(func(argv []string, i int, b string) (int, error) { return ArgSource(argv, i, b, &c.tabinfos) })
	c.AddParser(func(argv []string, i int, b string) (int, error) { return ArgTemplate(argv, i, b, &c.templates) })
	c.AddParser(func(argv []string, i int, b string) (int, error) { return ArgSelect(argv, i, b, &c.templates) })
	c.AddParser(func(argv []string, i int, b string) (int, error) { return ArgParam(argv, i, b, c.params) })
	c.AddParser(func(argv []string, i int, b string) (int, error) { return ArgSql(argv, i, b, &c.sqls) })
	c.AddParser(func(argv []string, i int, b string) (int, error) { return ArgAttach(argv, i, b, c.dbs) })
	c.AddParser(func(argv []string, i int, b string) (int, error) { return ArgIni(argv, i, b, &c.allargs) })
	c.AddParser(func(argv []string, i int, b string) (int, error) { return ArgDB(argv, i, b, &c.dbname) })
	c.AddParser(ArgIgnoreComment)
	c.AddParser(ArgIgnoreEmpty)
}

func (c *Config) Parse(argv []string) (err error) {
	if c.parsersready == false {
		c.Init()
	}
	var ap = argpart{}
	ap.basedir = "."
	ap.argv = argv
	c.allargs.parts = []argpart{ap}

	for pi := 0; pi < len(c.allargs.parts); pi++ {
		av := c.allargs.parts[pi].argv
		bd := c.allargs.parts[pi].basedir
		i := 0
		for {
			curr := i
			for _, p := range c.parsers {
				i, err = p(av, i, bd)
				if err != nil || i != curr {
					break
				}
			}
			if err != nil {
				return err
			}
			if curr == i {
				if i < len(av) {
					return fmt.Errorf("%d args left: %v", len(av)-i, av[i:])
				}
				break
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) Apply(m *Musql) (err error) {
	if c.dbname == "" {
		err = m.NewDb()
	} else {
		err = m.OpenDb(c.dbname)
	}
	if err != nil {
		return err
	}

	for _, t := range c.tabinfos {
		var err error
		var stat os.FileInfo
		if t.XPath != "" {
			if t.Type == "xml" || (t.Type == "" && len(t.Files) > 0 && strings.HasSuffix(t.Files[0].Path, ".xml")) {
				err = m.AddXml(t.Tablename, t.Files, t.XPath, t.XSelect)
			} else {
				err = m.AddJson(t.Tablename, t.Files, t.XPath, t.XSelect)
			}
		} else if stat, err = os.Stat(t.Files[0].Path); len(t.Files) == 1 && err == nil && stat.IsDir() {
			err = m.AddFiles(t.Tablename, t.Files[0].Path, t.Content)
		} else {
			if len(t.Header) > 0 {
				err = m.AddCsvWithHeader(t.Tablename, t.Files, t.Sep, t.Header)
			} else {
				err = m.AddCsv(t.Tablename, t.Files, t.Sep)
			}
		}
		if err != nil {
			return err
		}
	}
	err = m.AddParameters("parameter", c.params)
	if err != nil {
		return err
	}
	for fname, name := range c.dbs {
		err = m.AddDatabase(fname, name)
		if err != nil {
			return err
		}
	}
	for _, fname := range c.sqls {
		err := m.ApplySql(fname)
		if err != nil {
			return err
		}
	}
	for _, templ := range c.templates {
		var out *os.File
		if templ.Outname == "stdout" {
			out = os.Stdout
		} else {
			out, err = os.Create(templ.Outname)
			if err != nil {
				return err
			}
		}
		err := m.RunTemplate(string(templ.TemplateString), out)
		if err != nil {
			return err
		}
	}

	return nil
}
