package bolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const minKeysPerPage = 2

const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

const (
	bucketLeafFlag = 0x01
)

type pgid uint64

// page 结构体可以分为两部分：头信息：id、flags、count、overflow 字段，数据信息：ptr
type page struct {
	id       pgid    // id 页 id 8 字节
	flags    uint16  // flags 页类型，可以是分支，叶子节点，元信息，空闲列表，共 2 字节
	count    uint16  // count，记录此 page 中的元素个数
	overflow uint32  // overflow 4 字节，page 的溢出数量，溢出的 page 与当前 page 是在磁盘上连续存放的，因此 pageid 也是连续的
	ptr      uintptr // ptr	指向 page 数据的内存地址
}

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// 如果 page 存储着 meta 信息，那么通过此方式返回此 meta 结构体指针
// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// 注意：由于 page 可以 overflow，因此一个内存中的 page 可以对应着磁盘上多个连续存储的 page，因此 index 可以很大
// TODO 0x7FFFFFF 代表 2^27^-1，unit16 最多表示区间 [0,2^16^-1] 返回内的数字，所以使用 0x7FFFFFF 的原因是什么？
// 如果 page 代表的是 leaf，那么这个方法返回其中指定 index 对应的 node 元素，即 leafPageElement 结构体
// leafPageElement retrieves the leaf node by index
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// 如果 page 代表 leaf，返回整个 leafPageElement 数组
// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// 如果 page 代表 branch，返回指定 index 下的 node 元素，即 branchPageElement 结构体
// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// 如果 page 代表 branch，返回整个 branchPageElement 数组
// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// 就是简单地将 p 的字节码直接序列化输出
// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

// pages 代表的是可以进行排序的 *page 数组
type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement 是 branch 每一个元素的元信息，每一个 branch 的实际数据存储于偏移量为 pos 的节点元素上
// branchPageElement represents a node on a branch page.
type branchPageElement struct {
	pos   uint32 // pos 为 key 实际存储位置于当前 branchPageElement 实例的偏移量
	ksize uint32 // key 对应 []byte 的长度
	pgid  pgid   // pgid 即 page id，代表含有该 branchPageElement 的 page id
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	// 首先将 branchPageElement 元素指针转换为 []byte 数组的地址
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	// 然后得到相对 branchPageElement 偏移量 pos 处的地址为起点的 [0,ksize) 范围内的 byte 元素数组，最后返回
	// 第二个 :n.ksize 表示这个切片的 cap 大小也限制为 n.ksize
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// leafPageElement represents a node on a leaf page.
type leafPageElement struct {
	flags uint32 // 叶子节点的类型
	pos   uint32 // 具体 key-value 字节相对于 leafPageElement 节点的偏移量
	ksize uint32 // key 的字节大小
	vsize uint32 // value 的字节大小
}
// leafPageElement 上读取 key value 的和 branchPageElement 是一样的
// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}
// pgids 就是一个可以排序的 pgid 数组
type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge 用于合并两个 pgid 数组
// merge returns the sorted union of a and b.
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}
