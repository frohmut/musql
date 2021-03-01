package internal

import (
	"bytes"
	"testing"
)

func TestBasics(t *testing.T) {
	var m = &Musql{}
	m.NewDb()
	defer m.Close()
	err := m.AddCsv("mytable", []FileInfo{FileInfo{Path: "test/t.csv"}, FileInfo{Path: "test/t2.csv"}}, ';')
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestTableMismatch(t *testing.T) {
	var m = &Musql{}
	m.NewDb()
	defer m.Close()
	err := m.AddCsv("mytable_2", []FileInfo{FileInfo{Path: "test/t.csv"}, FileInfo{Path: "test/other_t.csv"}}, ';')
	if err == nil {
		t.Errorf("Expecting error")
	}
}

func TestMustache(t *testing.T) {
	var m = &Musql{}
	m.NewDb()
	defer m.Close()
	err := m.AddCsv("must", []FileInfo{FileInfo{Path: "test/t.csv"}}, ';')
	if err != nil {
		t.Errorf("%v", err)
	}
	out := bytes.NewBufferString("")
	err = m.RunTemplate(`
		{{#sql}}
		select Wert from must
		{{/sql}}
		{{#result}}
			{{Wert}}
		{{/result}}
	`, out)
	expect := `
			15000
			30000
			17000
	`
	if err != nil {
		t.Errorf("%v", err)
	}
	if out.String() != expect {
		t.Errorf("%s <> %s", out.String(), expect)
	}
}

func TestAttach(t *testing.T) {
	var m = &Musql{}
	m.NewDb()
	defer m.Close()
	err := m.AddDatabase("test/test.db", "test")
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestFileNotFound(t *testing.T) {
	var m = &Musql{}
	m.NewDb()
	defer m.Close()
	err := m.AddCsv("a", []FileInfo{FileInfo{Path: "no_file.csv"}}, ';')
	if err == nil {
		t.Errorf("should fail as no_file.csv does not exist")
	}
}

func BenchmarkSlowDB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var m = &Musql{}
		m.OpenDb("test_a.db")
		err := m.AddCsv("a", []FileInfo{FileInfo{Path: "test/a.csv"}}, ';')
		if err != nil {
			b.Errorf("%v", err)
		}
	}
}

func TestWith(t *testing.T) {
	var m = &Musql{}
	m.NewDb()
	defer m.Close()
	out := bytes.NewBufferString("")
	err := m.RunTemplate(`
		{{#sql}}
		with fragment t_t as (
		  select * from tt
		)
		{{/sql}}
		{{t_t}}
	`, out)
	if err != nil {
		t.Errorf("%v", err)
	}
	expect := `
		t_t as (
		  select * from tt
		)

	`
	if out.String() != expect {
		t.Errorf("bad: >%s< <> >%s<", out.String(), expect)
	}
}
