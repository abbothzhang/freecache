package freecache

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

const HASH_ENTRY_SIZE = 16
const ENTRY_HDR_SIZE = 24

var ErrLargeKey = errors.New("The key is larger than 65535")
var ErrLargeEntry = errors.New("The entry size is larger than 1/1024 of cache size")
var ErrNotFound = errors.New("Entry not found")

// entry pointer struct points to an entry in ring buffer
type entryPtr struct {
	offset   int64  // entry offset in ring buffer
	hash16   uint16 // entries are ordered by hash16 in a slot.
	keyLen   uint16 // used to compare a key
	reserved uint32
}

// entry header struct in ring buffer, followed by key and value.
type entryHdr struct {
	accessTime uint32
	expireAt   uint32
	keyLen     uint16
	hash16     uint16
	valLen     uint32
	valCap     uint32
	deleted    bool
	slotId     uint8
	reserved   uint16
}

// a segment contains 256 slots, a slot is an array of entry pointers ordered by hash16 value
// the entry can be looked up by hash value of the key.
type segment struct {
	rb            RingBuf // ring buffer that stores data
	segId         int
	_             uint32
	missCount     int64
	hitCount      int64
	entryCount    int64
	totalCount    int64      // number of entries in ring buffer, including deleted entries.
	totalTime     int64      // used to calculate least recent used entry.
	timer         Timer      // Timer giving current time
	totalEvacuate int64      // used for debug
	totalExpired  int64      // used for debug
	overwrites    int64      // used for debug
	touched       int64      // used for debug
	vacuumLen     int64      // up to vacuumLen, new data can be written without overwriting old data.
	slotLens      [256]int32 // The actual length for every slot.
	slotCap       int32      // max number of entry pointers a slot can hold.
	slotsData     []entryPtr // shared by all 256 slots
}

func newSegment(bufSize int, segId int, timer Timer) (seg segment) {
	seg.rb = NewRingBuf(bufSize, 0)
	seg.segId = segId
	seg.timer = timer
	seg.vacuumLen = int64(bufSize)
	seg.slotCap = 1
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	return
}

func (seg *segment) set(key, value []byte, hashVal uint64, expireSeconds int) (err error) {
	// 检查键的长度是否超过最大限制（65535字节）
	if len(key) > 65535 {
		return ErrLargeKey
	}

	// key和value的总长度不能超过freecache预设大小的1/1024
	maxKeyValLen := len(seg.rb.data)/4 - ENTRY_HDR_SIZE
	if len(key)+len(value) > maxKeyValLen {
		// 如果超出限制，返回错误
		return ErrLargeEntry
	}

	// 获取当前时间
	now := seg.timer.Now()
	expireAt := uint32(0)
	// 如果设置了过期时间，则计算过期时间点
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	// 根据哈希值计算槽ID和哈希的低16位
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	// 获取指定槽ID的槽
	slot := seg.getSlot(slotId)
	// 查找键在槽中的位置
	idx, match := seg.lookup(slot, hash16, key)

	// 创建一个固定大小的缓冲区用于存储条目的头部信息
	var hdrBuf [ENTRY_HDR_SIZE]byte
	// 将缓冲区转换为entryHdr类型的指针以方便操作
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))

	if match {
		// 如果键已存在，读取当前条目的头部信息
		matchedPtr := &slot[idx]
		seg.rb.ReadAt(hdrBuf[:], matchedPtr.offset)
		hdr.slotId = slotId
		hdr.hash16 = hash16
		hdr.keyLen = uint16(len(key))
		originAccessTime := hdr.accessTime
		hdr.accessTime = now
		hdr.expireAt = expireAt
		hdr.valLen = uint32(len(value))
		if hdr.valCap >= hdr.valLen {
			// 如果条目的容量足够，直接覆盖旧值
			atomic.AddInt64(&seg.totalTime, int64(hdr.accessTime)-int64(originAccessTime))
			seg.rb.WriteAt(hdrBuf[:], matchedPtr.offset)
			seg.rb.WriteAt(value, matchedPtr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
			atomic.AddInt64(&seg.overwrites, 1)
			return
		}
		// 如果容量不足，删除旧条目
		seg.delEntryPtr(slotId, slot, idx)
		match = false
		// 计算新容量
		for hdr.valCap < hdr.valLen {
			hdr.valCap *= 2
		}
		if hdr.valCap > uint32(maxKeyValLen-len(key)) {
			hdr.valCap = uint32(maxKeyValLen - len(key))
		}
	} else {
		// 如果键不存在，初始化头部信息
		hdr.slotId = slotId
		hdr.hash16 = hash16
		hdr.keyLen = uint16(len(key))
		hdr.accessTime = now
		hdr.expireAt = expireAt
		hdr.valLen = uint32(len(value))
		hdr.valCap = uint32(len(value))
		if hdr.valCap == 0 { // 避免容量为零的无限循环
			hdr.valCap = 1
		}
	}

	// 计算条目的总长度（包括头部、键和值）
	entryLen := ENTRY_HDR_SIZE + int64(len(key)) + int64(hdr.valCap)
	// 尝试从槽中腾出空间
	slotModified := seg.evacuate(entryLen, slotId, now)
	if slotModified {
		// 如果在腾出空间过程中槽被修改，需要重新查找键的位置
		slot = seg.getSlot(slotId)
		idx, match = seg.lookup(slot, hash16, key)
		// assert(match == false)
	}

	// 获取新的写入偏移量
	newOff := seg.rb.End()
	// 插入条目到槽中
	seg.insertEntryPtr(slotId, hash16, newOff, idx, hdr.keyLen)
	// 写入条目的头部、键和值到缓存中
	seg.rb.Write(hdrBuf[:])
	seg.rb.Write(key)
	seg.rb.Write(value)
	// 如果实际值长度小于分配的容量，跳过未使用的空间
	seg.rb.Skip(int64(hdr.valCap - hdr.valLen))
	// 更新统计数据
	atomic.AddInt64(&seg.totalTime, int64(now))
	atomic.AddInt64(&seg.totalCount, 1)
	// 减少可用的缓存空间
	seg.vacuumLen -= entryLen
	return
}

func (seg *segment) touch(key []byte, hashVal uint64, expireSeconds int) (err error) {
	if len(key) > 65535 {
		return ErrLargeKey
	}

	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		err = ErrNotFound
		return
	}
	matchedPtr := &slot[idx]

	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], matchedPtr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))

	now := seg.timer.Now()
	if isExpired(hdr.expireAt, now) {
		seg.delEntryPtr(slotId, slot, idx)
		atomic.AddInt64(&seg.totalExpired, 1)
		err = ErrNotFound
		atomic.AddInt64(&seg.missCount, 1)
		return
	}

	expireAt := uint32(0)
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	originAccessTime := hdr.accessTime
	hdr.accessTime = now
	hdr.expireAt = expireAt
	// in place overwrite
	atomic.AddInt64(&seg.totalTime, int64(hdr.accessTime)-int64(originAccessTime))
	seg.rb.WriteAt(hdrBuf[:], matchedPtr.offset)
	atomic.AddInt64(&seg.touched, 1)
	return
}

func (seg *segment) evacuate(entryLen int64, slotId uint8, now uint32) (slotModified bool) {
	var oldHdrBuf [ENTRY_HDR_SIZE]byte
	consecutiveEvacuate := 0
	for seg.vacuumLen < entryLen {
		oldOff := seg.rb.End() + seg.vacuumLen - seg.rb.Size()
		seg.rb.ReadAt(oldHdrBuf[:], oldOff)
		oldHdr := (*entryHdr)(unsafe.Pointer(&oldHdrBuf[0]))
		oldEntryLen := ENTRY_HDR_SIZE + int64(oldHdr.keyLen) + int64(oldHdr.valCap)
		if oldHdr.deleted {
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			continue
		}
		expired := isExpired(oldHdr.expireAt, now)
		leastRecentUsed := int64(oldHdr.accessTime)*atomic.LoadInt64(&seg.totalCount) <= atomic.LoadInt64(&seg.totalTime)
		if expired || leastRecentUsed || consecutiveEvacuate > 5 {
			seg.delEntryPtrByOffset(oldHdr.slotId, oldHdr.hash16, oldOff)
			if oldHdr.slotId == slotId {
				slotModified = true
			}
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			if expired {
				atomic.AddInt64(&seg.totalExpired, 1)
			} else {
				atomic.AddInt64(&seg.totalEvacuate, 1)
			}
		} else {
			// evacuate an old entry that has been accessed recently for better cache hit rate.
			newOff := seg.rb.Evacuate(oldOff, int(oldEntryLen))
			seg.updateEntryPtr(oldHdr.slotId, oldHdr.hash16, oldOff, newOff)
			consecutiveEvacuate++
			atomic.AddInt64(&seg.totalEvacuate, 1)
		}
	}
	return
}

func (seg *segment) get(key, buf []byte, hashVal uint64, peek bool) (value []byte, expireAt uint32, err error) {
	hdr, ptrOffset, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return
	}
	expireAt = hdr.expireAt
	if cap(buf) >= int(hdr.valLen) {
		value = buf[:hdr.valLen]
	} else {
		value = make([]byte, hdr.valLen)
	}

	seg.rb.ReadAt(value, ptrOffset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
	if !peek {
		atomic.AddInt64(&seg.hitCount, 1)
	}
	return
}

// view provides zero-copy access to the element's value, without copying to
// an intermediate buffer.
func (seg *segment) view(key []byte, fn func([]byte) error, hashVal uint64, peek bool) (err error) {
	hdr, ptrOffset, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return
	}
	start := ptrOffset + ENTRY_HDR_SIZE + int64(hdr.keyLen)
	val, err := seg.rb.Slice(start, int64(hdr.valLen))
	if err != nil {
		return err
	}
	err = fn(val)
	if !peek {
		atomic.AddInt64(&seg.hitCount, 1)
	}
	return
}

func (seg *segment) locate(key []byte, hashVal uint64, peek bool) (hdrEntry entryHdr, ptrOffset int64, err error) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		err = ErrNotFound
		if !peek {
			atomic.AddInt64(&seg.missCount, 1)
		}
		return
	}
	ptr := &slot[idx]

	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], ptr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if !peek {
		now := seg.timer.Now()
		if isExpired(hdr.expireAt, now) {
			seg.delEntryPtr(slotId, slot, idx)
			atomic.AddInt64(&seg.totalExpired, 1)
			err = ErrNotFound
			atomic.AddInt64(&seg.missCount, 1)
			return
		}
		atomic.AddInt64(&seg.totalTime, int64(now-hdr.accessTime))
		hdr.accessTime = now
		seg.rb.WriteAt(hdrBuf[:], ptr.offset)
	}
	return *hdr, ptr.offset, nil
}

func (seg *segment) del(key []byte, hashVal uint64) (affected bool) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		return false
	}
	seg.delEntryPtr(slotId, slot, idx)
	return true
}

func (seg *segment) ttl(key []byte, hashVal uint64) (timeLeft uint32, err error) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		err = ErrNotFound
		return
	}
	ptr := &slot[idx]

	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], ptr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))

	if hdr.expireAt == 0 {
		return
	} else {
		now := seg.timer.Now()
		if !isExpired(hdr.expireAt, now) {
			timeLeft = hdr.expireAt - now
			return
		}
	}
	err = ErrNotFound
	return
}

func (seg *segment) expand() {
	newSlotData := make([]entryPtr, seg.slotCap*2*256)
	for i := 0; i < 256; i++ {
		off := int32(i) * seg.slotCap
		copy(newSlotData[off*2:], seg.slotsData[off:off+seg.slotLens[i]])
	}
	seg.slotCap *= 2
	seg.slotsData = newSlotData
}

func (seg *segment) updateEntryPtr(slotId uint8, hash16 uint16, oldOff, newOff int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOff(slot, hash16, oldOff)
	if !match {
		return
	}
	ptr := &slot[idx]
	ptr.offset = newOff
}

func (seg *segment) insertEntryPtr(slotId uint8, hash16 uint16, offset int64, idx int, keyLen uint16) {
	if seg.slotLens[slotId] == seg.slotCap {
		seg.expand()
	}
	seg.slotLens[slotId]++
	atomic.AddInt64(&seg.entryCount, 1)
	slot := seg.getSlot(slotId)
	copy(slot[idx+1:], slot[idx:])
	slot[idx].offset = offset
	slot[idx].hash16 = hash16
	slot[idx].keyLen = keyLen
}

func (seg *segment) delEntryPtrByOffset(slotId uint8, hash16 uint16, offset int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOff(slot, hash16, offset)
	if !match {
		return
	}
	seg.delEntryPtr(slotId, slot, idx)
}

func (seg *segment) delEntryPtr(slotId uint8, slot []entryPtr, idx int) {
	offset := slot[idx].offset
	var entryHdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(entryHdrBuf[:], offset)
	entryHdr := (*entryHdr)(unsafe.Pointer(&entryHdrBuf[0]))
	entryHdr.deleted = true
	seg.rb.WriteAt(entryHdrBuf[:], offset)
	copy(slot[idx:], slot[idx+1:])
	seg.slotLens[slotId]--
	atomic.AddInt64(&seg.entryCount, -1)
}

func entryPtrIdx(slot []entryPtr, hash16 uint16) (idx int) {
	high := len(slot)
	for idx < high {
		mid := (idx + high) >> 1
		oldEntry := &slot[mid]
		if oldEntry.hash16 < hash16 {
			idx = mid + 1
		} else {
			high = mid
		}
	}
	return
}

func (seg *segment) lookup(slot []entryPtr, hash16 uint16, key []byte) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match = int(ptr.keyLen) == len(key) && seg.rb.EqualAt(key, ptr.offset+ENTRY_HDR_SIZE)
		if match {
			return
		}
		idx++
	}
	return
}

func (seg *segment) lookupByOff(slot []entryPtr, hash16 uint16, offset int64) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match = ptr.offset == offset
		if match {
			return
		}
		idx++
	}
	return
}

func (seg *segment) resetStatistics() {
	atomic.StoreInt64(&seg.totalEvacuate, 0)
	atomic.StoreInt64(&seg.totalExpired, 0)
	atomic.StoreInt64(&seg.overwrites, 0)
	atomic.StoreInt64(&seg.hitCount, 0)
	atomic.StoreInt64(&seg.missCount, 0)
}

func (seg *segment) clear() {
	bufSize := len(seg.rb.data)
	seg.rb.Reset(0)
	seg.vacuumLen = int64(bufSize)
	seg.slotCap = 1
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	for i := 0; i < len(seg.slotLens); i++ {
		seg.slotLens[i] = 0
	}

	atomic.StoreInt64(&seg.hitCount, 0)
	atomic.StoreInt64(&seg.missCount, 0)
	atomic.StoreInt64(&seg.entryCount, 0)
	atomic.StoreInt64(&seg.totalCount, 0)
	atomic.StoreInt64(&seg.totalTime, 0)
	atomic.StoreInt64(&seg.totalEvacuate, 0)
	atomic.StoreInt64(&seg.totalExpired, 0)
	atomic.StoreInt64(&seg.overwrites, 0)
}

func (seg *segment) getSlot(slotId uint8) []entryPtr {
	slotOff := int32(slotId) * seg.slotCap
	return seg.slotsData[slotOff : slotOff+seg.slotLens[slotId] : slotOff+seg.slotCap]
}

// isExpired checks if a key is expired.
func isExpired(keyExpireAt, now uint32) bool {
	return keyExpireAt != 0 && keyExpireAt <= now
}
