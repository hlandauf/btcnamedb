package nameldb

import "fmt"
import "os"
import "sync"
import "bytes"
import "encoding/binary"
import "github.com/hlandauf/btcnamedb"
import "github.com/hlandauf/btcwire"
import "github.com/conformal/goleveldb/leveldb"
import "github.com/conformal/goleveldb/leveldb/cache"
import "github.com/conformal/goleveldb/leveldb/opt"
import dbutil "github.com/conformal/goleveldb/leveldb/util"

// Schema:
//   "n" + name   = NameInfo (at current height)
//   "h" + name   = []NameInfo (history)
//   "x" + height + name = ()

type LevelDB struct {
	dbLock sync.Mutex

	db *leveldb.DB
	ro *opt.ReadOptions
	wo *opt.WriteOptions

	lbatch *leveldb.Batch
}

var self = btcnamedb.DriverDB{DBType: "leveldb", CreateDB: CreateDB, OpenDB: OpenDB}

func init() {
	btcnamedb.AddDBDriver(self)
}

func parseArgs(funcName string, args ...interface{}) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("Invalid arguments to nameldb.%s -- "+
			"expected database path string", funcName)
	}

	dbPath, ok := args[0].(string)
	if !ok {
		return "", fmt.Errorf("First argument to nameldb.%s is invalid -- "+
			"expected database path string", funcName)
	}

	return dbPath, nil
}

func OpenDB(args ...interface{}) (btcnamedb.DB, error) {
	dbpath, err := parseArgs("OpenDB", args...)
	if err != nil {
		return nil, err
	}

	db, err := openDB(dbpath, false)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func CreateDB(args ...interface{}) (btcnamedb.DB, error) {
	dbpath, err := parseArgs("Create", args...)
	if err != nil {
		return nil, err
	}

	db, err := openDB(dbpath, true)
	if err != nil {
		return nil, err
	}

	return db, nil
}

const CurrentDBVersion int32 = 0

func openDB(dbpath string, create bool) (pbdb btcnamedb.DB, err error) {
	var db LevelDB
	var tlDB *leveldb.DB
	var dbversion int32

	defer func() {
		if err == nil {
			db.db = tlDB
			pbdb = &db
		}
	}()

	if create {
		err = os.Mkdir(dbpath, 0750)
		if err != nil {
			//log.Errorf("mkdir failed %v %v", dbpath, err)
			return
		}
	} else {
		_, err = os.Stat(dbpath)
		if err != nil {
			err = btcnamedb.ErrDBDoesNotExist
			return
		}
	}

	needVersionFile := false
	verfile := dbpath + ".ver"
	fi, ferr := os.Open(verfile)
	if ferr == nil {
		defer fi.Close()

		ferr = binary.Read(fi, binary.LittleEndian, &dbversion)
		if ferr != nil {
			dbversion = ^0
		}
	} else if create {
		needVersionFile = true
		dbversion = CurrentDBVersion
	}

	myCache := cache.NewEmptyCache()
	opts := &opt.Options{
		BlockCache:   myCache,
		MaxOpenFiles: 256,
		//Compression:  opt.NoCompression,
	}

	switch dbversion {
	case 0:
	default:
		err = fmt.Errorf("unsupported namedb version %v", dbversion)
		return
	}

	tlDB, err = leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return
	}

	if needVersionFile {
		fo, ferr := os.Create(verfile)
		if ferr != nil {
			err = ferr
			return
		}

		defer fo.Close()
		err = binary.Write(fo, binary.LittleEndian, dbversion)
		if err != nil {
			return
		}
	}

	return
}

func (db *LevelDB) close() error {
	return db.db.Close()
}

func (db *LevelDB) Sync() error {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return nil
}

func (db *LevelDB) Close() error {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return db.close()
}

func (db *LevelDB) Set(nameItem btcwire.NameInfo) (err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	defer func() {
		if err == nil {
			err = db.processBatches()
		} else {
			db.lBatch().Reset()
		}
	}()

  //fmt.Printf("*** NAME SET: %#v\n", nameItem.Key)

  oldNameInfo, err := db.getByKey(nameItem.Key)
  if err == nil {
    // had an old name
    err = db.eraseHeightKey(nameItem.Key, oldNameInfo.Height)
    if err != nil {
      return err
    }
  }

	storageKey := "n" + nameItem.Key

	err = db.insertNameData(storageKey, &nameItem)
	if err != nil {
		fmt.Printf("*** Failed to insert name %#v %v\n", storageKey, err)
		return err
	}

  historyStorageKey := "h" + nameItem.Key
  err = db.appendToHistory(historyStorageKey, &nameItem)
  if err != nil {
    fmt.Printf("*** Failed to add to history\n")
    return err
  }

  heightStorageKey := make([]byte, 9+len(nameItem.Key))
  heightStorageKey[0] = 'x'
  binary.BigEndian.PutUint64(heightStorageKey[1:], uint64(nameItem.Height))
  copy(heightStorageKey[9:], []byte(nameItem.Key))

  err = db.insertHeightItem(heightStorageKey)
  if err != nil {
    fmt.Printf("*** Failed to add height item\n")
    return err
  }

	return nil
}

func (db *LevelDB) insertNameData(storageKey string, nameItem *btcwire.NameInfo) error {
	b := new(bytes.Buffer)
	err := nameItem.Serialize(b)

	if err != nil {
		return err
	}

	db.lBatch().Put([]byte(storageKey), b.Bytes())
	return nil
}

func (db *LevelDB) appendToHistory(storageKey string, nameItem *btcwire.NameInfo) error {
  return db.reserializeHistory(storageKey, func(items *[]btcwire.NameInfo) error {
    *items = append(*items, *nameItem)
    return nil
  })
}

func (db *LevelDB) reserializeHistory(storageKey string, transmuteFunc func(items *[]btcwire.NameInfo) error) (err error) {
  names := []btcwire.NameInfo{}

  historyData, err := db.db.Get([]byte(storageKey), db.ro)
  if err != nil && err != leveldb.ErrNotFound {
    return
  }

  r := bytes.NewReader(historyData)
  for r.Len() > 0 {
    ni := btcwire.NameInfo{}
    err = ni.Deserialize(r)
    if err != nil {
      return err
    }

    names = append(names, ni)
  }

  err = transmuteFunc(&names)
  if err != nil {
    return
  }

  if len(names) == 0 {
    db.lBatch().Delete([]byte(storageKey))
    return nil
  }

  b := new(bytes.Buffer)
  for i := range names {
    err = names[i].Serialize(b)
    if err != nil {
      return
    }
  }

  db.lBatch().Put([]byte(storageKey), b.Bytes())

  return nil
}

func (db *LevelDB) insertHeightItem(heightStorageKey []byte) error {
  db.lBatch().Put(heightStorageKey, []byte{})
  return nil
}

func (db *LevelDB) eraseHeightKey(nameKey string, height int64) error {
  heightStorageKey := make([]byte, 9+len(nameKey))
  heightStorageKey[0] = 'x'
  binary.BigEndian.PutUint64(heightStorageKey[1:], uint64(height))
  copy(heightStorageKey[9:], []byte(nameKey))

  db.lBatch().Delete(heightStorageKey)
  return nil
}

func (db *LevelDB) GetByKey(key string) (nameItem btcwire.NameInfo, err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

  return db.getByKey(key)
}

func (db *LevelDB) getByKey(key string) (nameItem btcwire.NameInfo, err error) {
	storageKey := "n" + key

	data, err := db.db.Get([]byte(storageKey), db.ro)
	if err != nil {
		if err == leveldb.ErrNotFound {
			err = btcnamedb.ErrNameMissing
		}
		return
	}

	// deserialize
	r := bytes.NewReader(data)
	err = nameItem.Deserialize(r)
	if err != nil {
		return
	}

	return
}

func (db *LevelDB) GetHistoryByKey(key string) (nameItems []btcwire.NameInfo, err error) {
  db.dbLock.Lock()
  defer db.dbLock.Unlock()

  storageKey := "h" + key

  data, err := db.db.Get([]byte(storageKey), db.ro)
  if err != nil {
    if err == leveldb.ErrNotFound {
      err = btcnamedb.ErrNameMissing
    }
    return
  }

  r := bytes.NewReader(data)
  for r.Len() > 0 {
    ni := btcwire.NameInfo{}
    err = ni.Deserialize(r)
    if err != nil {
      return
    }

    nameItems = append(nameItems, ni)
  }

  return
}

func (db *LevelDB) GetNamesAtHeight(height int64) (names []string, err error) {
  db.dbLock.Lock()
  defer db.dbLock.Unlock()

  storageKey := make([]byte, 9)
  storageKey[0] = 'x'
  binary.BigEndian.PutUint64(storageKey[1:], uint64(height))

  endStorageKey := make([]byte, 9)
  endStorageKey[0] = 'x'
  binary.BigEndian.PutUint64(endStorageKey[1:], uint64(height)+1)

  it := db.db.NewIterator(&dbutil.Range{
    Start: storageKey,
    Limit: endStorageKey,
  }, db.ro)
  defer it.Release()

  for it.Next() {
    k := it.Key()
    names = append(names, string(k[9:]))
  }

  return
}

func (db *LevelDB) DropAtHeight(height int64) (err error) {
  db.dbLock.Lock()
  defer db.dbLock.Unlock()

  defer func() {
		if err == nil {
			err = db.processBatches()
		} else {
			db.lBatch().Reset()
		}
	}()

  heightStorageKey := make([]byte, 9)
  heightStorageKey[0] = 'x'
  binary.BigEndian.PutUint64(heightStorageKey[1:], uint64(height))

  endKey := []byte{'y'}

  it := db.db.NewIterator(&dbutil.Range{
    Start: heightStorageKey,
    Limit: endKey,
  }, db.ro)
  defer it.Release()

  for it.Next() {
    k := it.Key()
    name := string(k[9:])

    db.lBatch().Delete(k)

    err = db.stripHistoryFromHeight("h" + name, height)
    if err != nil {
      return
    }
  }

  return
}

func (db *LevelDB) stripHistoryFromHeight(storageKey string, height int64) error {
  return db.reserializeHistory(storageKey, func(items *[]btcwire.NameInfo) error {
    newItems := make([]btcwire.NameInfo, 0, len(*items))
    for _, ni := range *items {
      if ni.Height < height {
        newItems = append(newItems, ni)
      }
    }

    *items = newItems
    return nil
  })
}

 
func (db *LevelDB) DeleteName(key string) (err error) {
  db.dbLock.Lock()
  defer db.dbLock.Unlock()

  defer func() {
    if err == nil {
      err = db.processBatches()
    } else {
      db.lBatch().Reset()
    }
  }()

  nameStorageKey := make([]byte, 1+len(key))
  nameStorageKey[0] = 'n'
  copy(nameStorageKey[1:], []byte(key))

  db.lBatch().Delete(nameStorageKey)

  return
}

func (db *LevelDB) lBatch() *leveldb.Batch {
	if db.lbatch == nil {
		db.lbatch = new(leveldb.Batch)
	}
	return db.lbatch
}

func (db *LevelDB) processBatches() error {
	var err error

	if db.lbatch != nil {
		if db.lbatch == nil {
			db.lbatch = new(leveldb.Batch)
		}

		defer db.lbatch.Reset()

		err = db.db.Write(db.lbatch, db.wo)
		if err != nil {
			//log.Tracef("batch failed %v\n", err)
			return err
		}
	}

	return nil
}

func (db *LevelDB) RollbackClose() error {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return db.close()
}
