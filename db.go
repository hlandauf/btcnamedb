package btcnamedb

import "github.com/hlandauf/btcwire"
import "fmt"

var (
	ErrDBUnknownType  = fmt.Errorf("non-existent database type")
	ErrDBDoesNotExist = fmt.Errorf("non-existent database")
	ErrNameMissing    = fmt.Errorf("name not found")
)

type DB interface {
	Close() (err error)

	Set(nameInfo btcwire.NameInfo) (err error)
	GetByKey(key string) (nameInfo btcwire.NameInfo, err error)
  GetHistoryByKey(key string) (nameInfo []btcwire.NameInfo, err error)
  GetNamesAtHeight(height int64) (nameInfo []string, err error)

  DropAtHeight(height int64) (err error)
  DeleteName(key string) (err error)

	RollbackClose() (err error)

	Sync() (err error)
}

type DriverDB struct {
	DBType   string
	CreateDB func(args ...interface{}) (pbdb DB, err error)
	OpenDB   func(args ...interface{}) (pbdb DB, err error)
}

var driverList []DriverDB

func AddDBDriver(instance DriverDB) {
	for _, drv := range driverList {
		if drv.DBType == instance.DBType {
			return
		}
	}

	driverList = append(driverList, instance)
}

func CreateDB(dbtype string, args ...interface{}) (pbdb DB, err error) {
	for _, drv := range driverList {
		if drv.DBType == dbtype {
			return drv.CreateDB(args...)
		}
	}
	return nil, ErrDBUnknownType
}

func OpenDB(dbtype string, args ...interface{}) (pbdb DB, err error) {
	for _, drv := range driverList {
		if drv.DBType == dbtype {
			return drv.OpenDB(args...)
		}
	}
	return nil, ErrDBUnknownType
}

func SupportedDBs() []string {
	var supportedDBs []string
	for _, drv := range driverList {
		supportedDBs = append(supportedDBs, drv.DBType)
	}
	return supportedDBs
}
