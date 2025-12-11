package executor

import (
	"testing"

	"tur/pkg/types"
	"tur/pkg/vdbe"
)

// BenchmarkMatchLikePattern benchmarks the LIKE pattern matching
func BenchmarkMatchLikePattern(b *testing.B) {
	testCases := []struct {
		name    string
		str     string
		pattern string
	}{
		{"exact", "hello world", "hello world"},
		{"prefix", "hello world", "hello%"},
		{"suffix", "hello world", "%world"},
		{"contains", "hello world", "%llo wor%"},
		{"underscore", "hello world", "hello_world"},
		{"complex", "hello beautiful world", "%hello%world%"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				matchLikePattern(tc.str, tc.pattern)
			}
		})
	}
}

// BenchmarkFindInSet benchmarks the FIND_IN_SET function
func BenchmarkFindInSet(b *testing.B) {
	registry := vdbe.DefaultFunctionRegistry()
	findInSet := registry.Lookup("FIND_IN_SET")

	testCases := []struct {
		name     string
		needle   string
		haystack string
	}{
		{"first", "apple", "apple,banana,cherry"},
		{"middle", "banana", "apple,banana,cherry"},
		{"last", "cherry", "apple,banana,cherry"},
		{"not_found", "grape", "apple,banana,cherry"},
		{"long_list", "item50", "item1,item2,item3,item4,item5,item6,item7,item8,item9,item10,item11,item12,item13,item14,item15,item16,item17,item18,item19,item20,item21,item22,item23,item24,item25,item26,item27,item28,item29,item30,item31,item32,item33,item34,item35,item36,item37,item38,item39,item40,item41,item42,item43,item44,item45,item46,item47,item48,item49,item50"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			args := []types.Value{types.NewText(tc.needle), types.NewText(tc.haystack)}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				findInSet.Call(args)
			}
		})
	}
}
