package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type kvEntry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	keyStore map[string]kvEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.keyStore = make(map[string]kvEntry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.keyStore[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = entry.value
	reply.Version = entry.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.keyStore[args.Key]
	if !ok {
		// version number larger than 0 and key doesn't exist
		// then return ErrNoKey
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		// note to self: do we enforce the args.Version == 0
		// on new Puts?
		// if we're here, and the version submitted is 0,
		//  we're starting a new kv entry, we set the version to 1
		if args.Version == 0 {
			kv.keyStore[args.Key] = kvEntry{value: args.Value, version: 1}
			reply.Err = rpc.OK
		}
		return
	}
	if entry.version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	entry.value = args.Value
	entry.version++
	kv.keyStore[args.Key] = entry
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
