package raft

import (
	"math/rand"
)

// RandomGenerator 生成0-n随机数，前n次保证全覆盖。
type RandomGenerator struct {
	n     int
	perm  []int // 0-n随机排列
	index int   // 当前已返回次数（0-n）
}

// NewRandomGenerator 创建生成器。
func NewRandomGenerator(n int) *RandomGenerator {
	return &RandomGenerator{
		n:     n,
		perm:  rand.Perm(n),
		index: 0,
	}
}

// Next 返回下一个随机数。
func (g *RandomGenerator) Next() int {
	if g.index < g.n {
		val := g.perm[g.index]
		g.index++
		return val
	}
	return rand.Intn(g.n)
}

// Reset 重置生成器，重新生成一个随机排列，计数器归零。
func (g *RandomGenerator) Reset() {
	g.perm = rand.Perm(g.n)
	g.index = 0
}
