package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"

type (
	Logger interface{
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct{
		mutex sync.Mutex
		mutexes map[string]*sync.Mutex
		dir string
		log Logger
	}
)

type Options struct{
	Logger
}


type Address struct{
	Street string
	City string
	State string
	Country string
	ZipCode json.Number
}

type User struct{
	Name string
	Age json.Number
	Contact string
	Company string
	Address Address
}

func  New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil{
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir: dir,
		mutexes: make(map[string]*sync.Mutex),
		log: opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil{
		opts.Logger.Debug("Using '%s' (database already exists)", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection string, resource string, value interface{}) error {
	if collection == ""{
		return fmt.Errorf("Missing Collection - No directory to save the record")
	}

	if resource == "" {
		return fmt.Errorf("Missing Resource - No name given to the record")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()
	dir := filepath.Join(d.dir, collection)
	finalPath := filepath.Join(dir, resource+".json")
	tempPath := finalPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	
	b, err := json.MarshalIndent(value, "", "\t")
	if err != nil{
		return err
	}

	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tempPath, b, 0644); err != nil{
		return err
	}

	return os.Rename(tempPath, finalPath)
}

func (d *Driver) Read(collection string, resource string, value interface{}) error {
	if collection == ""{
		return fmt.Errorf("Missing Collection - No Directory to store record")
	}

	if resource == ""{
		return fmt.Errorf("Missing Resource - No Name given to save record")
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil{
		return err
	}

	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &value)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing Collection - No Directory given to read from")
	}

	dir := filepath.Join(d.dir, collection)
	
	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, _ := ioutil.ReadDir(dir)

	var records []string

	for _, file := range files{
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) ReadAndCreate(collection string) {
	
}

func (d *Driver) Delete(collection string, resource string) error {
	if collection == "" {
		return fmt.Errorf("Missing Collection - No Directory given to find record")
	}

	if resource == "" {
		return fmt.Errorf("Missing Resource - No File Name given to delete")
	}

	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)
	switch fi, err := stat(dir); {
	case fi==nil, err!=nil: 
		return fmt.Errorf("Unable to find file or directory")
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}


func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex{
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func stat(path string) (fi os.FileInfo, err error){
	if fi, err = os.Stat(path); os.IsNotExist((err)) {
		fi, err = os.Stat(path + ".json")
	}
	return fi, nil
}




func main(){
	dir := "./"

	db, err := New(dir, nil)
	if err != nil{
		fmt.Println("Error", err)
	}


	NewAddress := Address{"Example", "Example City", "Example State", "USA", "11111"}

	usersWrite := []User{
		{"TeAndre Smith", "23", "08064807641", "Google", NewAddress},
		{"John", "23", "08064807641", "Google", NewAddress},
		{"Timmy", "23", "08064807641", "Google", NewAddress},
	}

	for _, user := range usersWrite {
		db.Write("users", user.Name, user)
	}

	fmt.Println(db)

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error", err)
	}
	fmt.Println(records)

	users := []User{}

	for _, f := range records{
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			fmt.Println("Error", err)
		}
		users = append(users, employeeFound)
	}

	db.Delete("users", "Timmy")
	records, err = db.ReadAll("users")
	if err != nil {
		fmt.Println("Error", err)
	}
	fmt.Println(records)
}

