package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string // name of the lock
	id       string // unique identifier for the client/holder of this lock
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		id:       kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// Your code here
		value, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.OK && value == lk.id {
			return // we hold the lock
		}
		if err == rpc.ErrNoKey || (err == rpc.OK && value == "") {
			if lk.ck.Put(lk.lockname, lk.id, version) == rpc.OK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond) // retrying
	}
}

func (lk *Lock) Release() {
	for {
		// Your code here
		value, version, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK || value != lk.id {
			return
		}
		if lk.ck.Put(lk.lockname, "", version) == rpc.OK {
			return
		}
		// loop
	}
}
